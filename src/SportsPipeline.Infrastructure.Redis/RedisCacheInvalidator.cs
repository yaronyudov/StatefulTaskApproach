using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Redis;

public sealed class RedisCacheInvalidator : ICacheInvalidator
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisCacheInvalidator> _logger;

    public RedisCacheInvalidator(IConnectionMultiplexer redis, ILogger<RedisCacheInvalidator> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    public async Task InvalidateMatchDetailsAsync(string matchId, CancellationToken cancellationToken)
    {
        try
        {
            var db = _redis.GetDatabase();
            // Note: The QueryApi uses IDistributedCache with InstanceName = "SportsPipeline:"
            // So the actual key in Redis is prepended with this prefix.
            var key = $"SportsPipeline:match-details:{matchId}";
            var deleted = await db.KeyDeleteAsync(key);
            
            if (deleted)
            {
                _logger.LogInformation("Invalidated Redis cache for match details {MatchId}", matchId);
            }
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to invalidate cache for match {MatchId}", matchId);
        }
    }
}
