using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Redis;

public sealed class RedisDeltaPublisher : IDeltaPublisher
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisDeltaPublisher> _logger;

    public RedisDeltaPublisher(IConnectionMultiplexer redis, ILogger<RedisDeltaPublisher> logger)
    {
        _redis = redis;
        _logger = logger;
    }

    public async Task PublishDeltaAsync(string matchId, object delta, CancellationToken cancellationToken)
    {
        try
        {
            var db = _redis.GetDatabase();
            var json = JsonSerializer.Serialize(delta);
            var channel = RedisChannel.Literal($"match-deltas:{matchId}");
            await db.PublishAsync(channel, json);
            _logger.LogDebug("Published delta to Redis channel match-deltas:{MatchId}", matchId);
        }
        catch (System.Exception ex)
        {
            _logger.LogError(ex, "Failed to publish delta for {MatchId} to Redis", matchId);
        }
    }
}
