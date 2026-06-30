using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Redis;

/// <summary>
/// Redis-backed <see cref="ILiveMatchStateStore"/>. The current details read-model is stored as a plain
/// string at <see cref="RedisKeys.LiveMatchDetails"/>; with AOF this survives a classifier-pod crash, so
/// it is safe to treat as the source of truth for live matches.
/// </summary>
public sealed class RedisLiveMatchStateStore : ILiveMatchStateStore
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisLiveMatchStateStore> _logger;

    public RedisLiveMatchStateStore(IConnectionMultiplexer redis, ILogger<RedisLiveMatchStateStore> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    public async Task WriteAsync(string matchId, string detailsJson, CancellationToken cancellationToken = default)
    {
        try
        {
            await _redis.GetDatabase().StringSetAsync(RedisKeys.LiveMatchDetails(matchId), detailsJson);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write live state for {MatchId}", matchId);
            throw; // live write-through must not silently fail: the caller persists before notifying
        }
    }

    public async Task<string?> ReadAsync(string matchId, CancellationToken cancellationToken = default)
    {
        var value = await _redis.GetDatabase().StringGetAsync(RedisKeys.LiveMatchDetails(matchId));
        return value.IsNullOrEmpty ? null : value.ToString();
    }

    public async Task EvictAsync(string matchId, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        try
        {
            // Don't hard-delete: leave a short grace TTL so in-flight readers still see the final state.
            await _redis.GetDatabase().KeyExpireAsync(RedisKeys.LiveMatchDetails(matchId), ttl);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to set TTL on live state for {MatchId}", matchId);
        }
    }
}
