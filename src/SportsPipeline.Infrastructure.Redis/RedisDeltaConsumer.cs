using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using StackExchange.Redis;

namespace SportsPipeline.Infrastructure.Redis;

/// <summary>
/// Consumes the match-deltas from Redis Pub/Sub (Orleans pipeline) and forwards each record to the injected <see cref="IDeltaHandler"/>.
/// This ensures the SSE app uses the exact same IDeltaHandler logic whether running the Kafka/Flink pipeline or the Redis/Orleans pipeline.
/// </summary>
public sealed class RedisDeltaConsumer : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IDeltaHandler _handler;
    private readonly ILogger<RedisDeltaConsumer> _logger;

    public RedisDeltaConsumer(IConnectionMultiplexer redis, IDeltaHandler handler, ILogger<RedisDeltaConsumer> logger)
    {
        _redis = redis;
        _handler = handler;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriber = _redis.GetSubscriber();
        var channel = new RedisChannel("match-deltas:*", RedisChannel.PatternMode.Pattern);
        
        await subscriber.SubscribeAsync(channel, (ch, message) =>
        {
            try
            {
                // ch name looks like "match-deltas:match123"
                var channelName = ch.ToString();
                var matchId = channelName.Substring("match-deltas:".Length);
                
                _handler.Handle(matchId, message.ToString());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing delta from Redis for channel {Channel}", ch);
            }
        });

        _logger.LogInformation("RedisDeltaConsumer subscribed to Redis pattern match-deltas:*");

        // Wait until cancelled
        var tcs = new TaskCompletionSource();
        stoppingToken.Register(() => 
        {
            subscriber.UnsubscribeAll();
            tcs.TrySetResult();
        });
        await tcs.Task;
    }
}
