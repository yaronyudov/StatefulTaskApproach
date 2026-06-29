using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;
using SportsPipeline.Contracts;

namespace SportsPipeline.Infrastructure.Kafka;

/// <summary>
/// Consumes raw-events from Kafka and delegates them to the injected <see cref="IRawEventHandler"/>.
/// </summary>
public sealed class KafkaRawEventConsumer(
    IRawEventHandler handler,
    IOptions<KafkaOptions> options,
    ILogger<KafkaRawEventConsumer> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = "raw-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(options.Value.ConsumeTopic);
        logger.LogInformation("KafkaRawEventConsumer subscribed to {ConsumeTopic} topic.", options.Value.ConsumeTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message?.Value is not null)
                {
                    try
                    {
                        if (!result.Message.Headers.TryGetLastBytes(Constants.KafkaHeaders.ProviderId, out var providerIdBytes))
                        {
                            logger.LogWarning("Message missing {HeaderName} header. Skipping.", Constants.KafkaHeaders.ProviderId);
                            continue;
                        }
                        var providerId = System.Text.Encoding.UTF8.GetString(providerIdBytes);

                        await handler.HandleAsync(providerId, result.Message.Value, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to process raw event.");
                    }
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
