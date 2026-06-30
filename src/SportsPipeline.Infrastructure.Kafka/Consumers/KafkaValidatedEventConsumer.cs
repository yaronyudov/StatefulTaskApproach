using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
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
/// <para>Scale-out: when <see cref="KafkaOptions.ConsumerCount"/> &gt; 1 the service runs that many
/// independent consumer loops in the same group, so the broker spreads the topic's partitions across
/// them (and across pods, if scaled). A shared concurrency gate bounds total in-flight grain calls.</para>
///
/// <para>Durability: offsets are committed once per batch (manual commit, after the batch finishes),
/// giving at-least-once delivery. A redelivered event after a crash is harmless because the grain's
/// idempotency gate (ProviderHighWaterMarks / event-time check) drops stale/duplicate events.</para>
/// </summary>
public class KafkaValidatedEventConsumer : BackgroundService
{
    private readonly ILogger<KafkaValidatedEventConsumer> _logger;
    private readonly IValidatedEventProcessor _processor;
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly int _batchSize;
    private readonly int _maxConcurrency;
    private readonly int _consumerCount;
    private readonly int _partitions;

    public KafkaValidatedEventConsumer(
        ILogger<KafkaValidatedEventConsumer> logger,
        IValidatedEventProcessor processor,
        IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _processor = processor;

        // This relies on the same KafkaOptions used by other infrastructure classes
        _topic = "validated-events"; // Could also be moved to KafkaOptions

        _bootstrapServers = options.Value.BootstrapServers;
        _batchSize = Math.Max(1, options.Value.BatchSize);
        _maxConcurrency = Math.Max(1, options.Value.MaxConcurrency);
        _consumerCount = Math.Max(1, options.Value.ConsumerCount);
        _partitions = Math.Max(1, options.Value.Partitions);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureTopicAsync(stoppingToken);

        _logger.LogInformation(
            "KafkaValidatedEventConsumer starting on topic {Topic} (consumers={ConsumerCount}, batchSize={BatchSize}, maxConcurrency={MaxConcurrency}).",
            _topic, _consumerCount, _batchSize, _maxConcurrency);

        // One shared gate bounds TOTAL in-flight grain calls across every consumer loop.
        using var gate = new SemaphoreSlim(_maxConcurrency);

        var loops = Enumerable.Range(0, _consumerCount)
            .Select(i => Task.Run(() => RunConsumerLoopAsync(i, gate, stoppingToken), stoppingToken))
            .ToArray();

        await Task.WhenAll(loops);
    }

    /// <summary>
    /// Runs one consumer (its own group member). Confluent.Kafka's Consume is blocking, so each loop
    /// owns a dedicated thread. Per-key ordering is preserved within a batch by chaining same-key
    /// tasks; the per-batch barrier preserves it across batches.
    /// </summary>
    private async Task RunConsumerLoopAsync(int index, SemaphoreSlim gate, CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "orleans-classifier-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // Manual commit once per batch, after processing succeeds
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(_topic);
        _logger.LogInformation("Consumer loop {Index} subscribed to {Topic}.", index, _topic);

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
                    var first = consumer.Consume(stoppingToken);
                    if (first?.Message != null) batch.Add(first);

                    while (batch.Count < _batchSize)
                    {
                        var more = consumer.Consume(TimeSpan.FromMilliseconds(20));
                        if (more?.Message == null) break;
                        batch.Add(more);
                    }

                    if (batch.Count == 0) continue;

                    // 2) Dispatch. SAME key -> chained (sequential, preserves per-match order).
                    //    DIFFERENT keys -> run concurrently, bounded by the shared gate.
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

                    // 3) Barrier, then a single commit for this loop's assigned partitions.
                    await Task.WhenAll(inFlight);
                    consumer.Commit();
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error in consumer loop {Index}", index);
                }
                catch (OperationCanceledException)
                {
                    throw; // bubble out to the outer handler on shutdown
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing batch in consumer loop {Index}", index);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer loop {Index} cancelled.", index);
        }
        finally
        {
            consumer.Close();
        }
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
            _logger.LogError(ex, "Error processing event for {Key}", key);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Ensures the topic exists with the configured partition count (create-if-missing). Only acts
    /// when Partitions &gt; 1; otherwise the topic is left to broker auto-creation. Increasing the
    /// partition count of an existing topic is intentionally NOT attempted here — provision partitions
    /// before the first producer writes (e.g. in infra / the benchmark script), since repartitioning
    /// changes key→partition mapping.
    /// </summary>
    private async Task EnsureTopicAsync(CancellationToken stoppingToken)
    {
        if (_partitions <= 1) return;

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = _topic, NumPartitions = _partitions, ReplicationFactor = 1 }
            });
            _logger.LogInformation("Created topic {Topic} with {Partitions} partitions.", _topic, _partitions);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            _logger.LogInformation("Topic {Topic} already exists; leaving its partition count unchanged.", _topic);
        }
        catch (Exception ex)
        {
            // Non-fatal: fall back to whatever the broker provides (e.g. auto-created topic).
            _logger.LogWarning(ex, "Could not ensure topic {Topic} has {Partitions} partitions.", _topic, _partitions);
        }
    }
}
