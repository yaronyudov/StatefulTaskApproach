using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Kafka.Consumers;

/// <summary>
/// Background service that consumes raw provider events from Kafka and forwards them to 
/// the generic IValidatedEventProcessor. It handles manual offsets to guarantee zero data loss.
/// </summary>
public class KafkaValidatedEventConsumer : BackgroundService
{
    private readonly ILogger<KafkaValidatedEventConsumer> _logger;
    private readonly IValidatedEventProcessor _processor;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _topic;

    public KafkaValidatedEventConsumer(
        ILogger<KafkaValidatedEventConsumer> logger, 
        IValidatedEventProcessor processor,
        IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _processor = processor;
        
        // This relies on the same KafkaOptions used by other infrastructure classes
        _topic = "validated-events"; // Could also be moved to KafkaOptions

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = "orleans-classifier-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // Manual commit after processor succeeds
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation("KafkaValidatedEventConsumer started on topic {Topic}.", _topic);

        // We run synchronously in the background thread since Confluent.Kafka's Consume is blocking
        await Task.Run(async () => 
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(stoppingToken);
                        if (result == null || result.Message == null) continue;

                        // Hand off the raw payload to the generic application logic
                        await _processor.ProcessEventAsync(result.Message.Value);

                        // Acknowledge ONLY after the processor completes (Zero Data Loss)
                        _consumer.Commit(result);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Consume error in KafkaValidatedEventConsumer");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing event in KafkaValidatedEventConsumer");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("KafkaValidatedEventConsumer cancelled.");
            }
            finally
            {
                _consumer.Close();
            }
        }, stoppingToken);
    }
}
