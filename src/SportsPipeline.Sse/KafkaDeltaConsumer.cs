using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SportsPipeline.Sse;

public sealed class SseOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = SportsPipeline.Contracts.Topics.MatchDeltas;
}

/// <summary>
/// Consumes the <c>match-deltas</c> topic and republishes each record to local SSE subscribers via
/// <see cref="DeltaBroker"/>. A unique consumer group per process instance means every instance
/// receives every delta, so any instance can serve any subscriber without a shared backplane.
/// </summary>
public sealed class KafkaDeltaConsumer(DeltaBroker broker, SseOptions options, ILogger<KafkaDeltaConsumer> logger)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run the blocking consume loop on a background thread so host startup is not blocked.
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            // Unique group per instance: each instance sees ALL partitions/deltas.
            GroupId = $"sse-{Environment.MachineName}-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(options.Topic);
        logger.LogInformation("SSE delta consumer subscribed to {Topic}.", options.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message is null)
                {
                    continue;
                }

                // Kafka key is the matchId (set by the Flink sink); value is the delta JSON.
                broker.Publish(result.Message.Key, result.Message.Value);
            }
        }
        catch (OperationCanceledException)
        {
            // graceful shutdown
        }
        finally
        {
            consumer.Close();
        }
    }
}
