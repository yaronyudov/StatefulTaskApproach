using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;

namespace SportsPipeline.QueryApi.Services;

/// <summary>
/// Provides Thundering Herd (Cache Stampede) protection using Request Coalescing.
/// Ensures that if 10,000 concurrent requests ask for the same cache key that is currently MISSING,
/// only ONE request will hit the underlying database. The other 9,999 requests will asynchronously await
/// the exact same Task and return simultaneously.
/// </summary>
public sealed class CacheStampedeProtector
{
    private readonly IDistributedCache _cache;
    private readonly ILogger<CacheStampedeProtector> _logger;
    private readonly ConcurrentDictionary<string, Task<string?>> _inFlightRequests = new();

    public CacheStampedeProtector(IDistributedCache cache, ILogger<CacheStampedeProtector> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task<string?> GetOrAddAsync(string key, Func<Task<string?>> dbFactory, TimeSpan? absoluteExpiration = null, CancellationToken ct = default)
    {
        // 1. Fast path: try cache first
        var cachedValue = await _cache.GetStringAsync(key, ct);
        if (cachedValue != null)
        {
            return cachedValue;
        }

        // 2. Cache miss. Coalesce requests by sharing the same Task
        var fetchTask = _inFlightRequests.GetOrAdd(key, k => FetchFromDbAsync(k, dbFactory, absoluteExpiration, ct));

        return await fetchTask;
    }

    private async Task<string?> FetchFromDbAsync(string key, Func<Task<string?>> dbFactory, TimeSpan? absoluteExpiration, CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Cache miss for {Key}. Hitting database (only 1 request allowed).", key);

            var dbValue = await dbFactory();

            if (dbValue != null)
            {
                var options = new DistributedCacheEntryOptions();
                if (absoluteExpiration.HasValue)
                {
                    options.SetAbsoluteExpiration(absoluteExpiration.Value);
                }
                
                await _cache.SetStringAsync(key, dbValue, options, ct);
            }

            return dbValue;
        }
        finally
        {
            // Remove the task from the dictionary once it's complete,
            // so future cache misses will trigger a new fetch.
            _inFlightRequests.TryRemove(key, out _);
        }
    }
}
