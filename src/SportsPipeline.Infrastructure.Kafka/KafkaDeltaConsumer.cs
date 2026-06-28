using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Kafka;

/// <summary>
/// Consumes the match-deltas topic and forwards each record to the injected <see cref="IDeltaHandler"/>
/// (the app supplies the handler). A unique consumer group per instance means every instance receives
/// every delta, so any instance can serve any subscriber without a shared backplane.
/// </summary>
public sealed class KafkaDeltaConsumer(KafkaConsumerOptions options, IDeltaHandler handler, ILogger<KafkaDeltaConsumer> logger)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = $"sse-{Environment.MachineName}-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(options.Topic);
        logger.LogInformation("Delta consumer subscribed to {Topic}.", options.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message is not null)
                {
                    handler.Handle(result.Message.Key, result.Message.Value);
                }
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
