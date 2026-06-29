using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SportsPipeline.Infrastructure.Kafka.Consumers;

/// <summary>
/// Background service that consumes validated events from Kafka and forwards them to the generic
/// IValidatedEventProcessor.
///
/// <para>Throughput: events are drained in batches and dispatched concurrently across matches while
/// events for the SAME match (the Kafka message key = MatchKey) are chained so they stay strictly
/// sequential. Per-match ordering is therefore preserved exactly as before — Kafka keys each match to
/// one partition and Orleans runs each grain single-threaded — but many different matches now make
/// progress at once instead of one event at a time globally.</para>
///
/// <para>Durability: offsets are committed once per batch (manual commit, after the batch finishes),
/// giving at-least-once delivery. A redelivered event after a crash is harmless because the grain's
/// idempotency gate (ProviderHighWaterMarks / event-time check) drops stale/duplicate events.</para>
/// </summary>
public class KafkaValidatedEventConsumer : BackgroundService
{
    private readonly ILogger<KafkaValidatedEventConsumer> _logger;
    private readonly IValidatedEventProcessor _processor;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _topic;
    private readonly int _batchSize;
    private readonly int _maxConcurrency;

    public KafkaValidatedEventConsumer(
        ILogger<KafkaValidatedEventConsumer> logger,
        IValidatedEventProcessor processor,
        IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _processor = processor;

        // This relies on the same KafkaOptions used by other infrastructure classes
        _topic = "validated-events"; // Could also be moved to KafkaOptions

        _batchSize = System.Math.Max(1, options.Value.BatchSize);
        _maxConcurrency = System.Math.Max(1, options.Value.MaxConcurrency);

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = "orleans-classifier-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // Manual commit once per batch, after processing succeeds
        };

        _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation(
            "KafkaValidatedEventConsumer started on topic {Topic} (batchSize={BatchSize}, maxConcurrency={MaxConcurrency}).",
            _topic, _batchSize, _maxConcurrency);

        // Bounds how many grain calls are in flight at once across the whole batch.
        using var gate = new SemaphoreSlim(_maxConcurrency);

        // Confluent.Kafka's Consume is blocking, so run the drain loop on a dedicated thread.
        await Task.Run(async () =>
        {
            try
            {
                var batch = new List<ConsumeResult<string, byte[]>>(_batchSize);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        // 1) Gather a batch: block for the first record, then drain whatever is
                        //    immediately available (don't wait around for a full batch to form).
                        batch.Clear();
                        var first = _consumer.Consume(stoppingToken);
                        if (first?.Message != null) batch.Add(first);

                        while (batch.Count < _batchSize)
                        {
                            var more = _consumer.Consume(TimeSpan.FromMilliseconds(20));
                            if (more?.Message == null) break;
                            batch.Add(more);
                        }

                        if (batch.Count == 0) continue;

                        // 2) Dispatch. SAME key -> chained (sequential, preserves per-match order).
                        //    DIFFERENT keys -> run concurrently, bounded by the gate.
                        var keyTails = new Dictionary<string, Task>(StringComparer.Ordinal);
                        var inFlight = new List<Task>(batch.Count);

                        foreach (var record in batch)
                        {
                            var key = record.Message.Key;
                            var payload = record.Message.Value;
                            var prev = keyTails.TryGetValue(key, out var tail) ? tail : Task.CompletedTask;

                            var next = prev.ContinueWith(
                                async _ => await ProcessOneAsync(key, payload, gate, stoppingToken),
                                CancellationToken.None,
                                TaskContinuationOptions.None,
                                TaskScheduler.Default).Unwrap();

                            keyTails[key] = next;
                            inFlight.Add(next);
                        }

                        // 3) Barrier, then a single commit for the whole batch.
                        await Task.WhenAll(inFlight);
                        _consumer.Commit(); // commits the current position for all assigned partitions
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Consume error in KafkaValidatedEventConsumer");
                    }
                    catch (OperationCanceledException)
                    {
                        throw; // bubble out to the outer handler on shutdown
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing batch in KafkaValidatedEventConsumer");
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

    /// <summary>
    /// Processes a single event with concurrency bounded by <paramref name="gate"/>. Exceptions are
    /// logged and swallowed so one bad event neither breaks its key's chain nor the batch barrier
    /// (at-least-once: the batch still commits; the grain's idempotency gate dedups on replay).
    /// </summary>
    private async Task ProcessOneAsync(string key, byte[] payload, SemaphoreSlim gate, CancellationToken stoppingToken)
    {
        await gate.WaitAsync(stoppingToken);
        try
        {
            await _processor.ProcessEventAsync(payload);
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing event for {Key} in KafkaValidatedEventConsumer", key);
        }
        finally
        {
            gate.Release();
        }
    }
}
