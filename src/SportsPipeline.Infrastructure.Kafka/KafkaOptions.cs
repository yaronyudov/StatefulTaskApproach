namespace SportsPipeline.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string PublishTopic { get; set; } = Topics.ValidatedEvents;
    public string ConsumeTopic { get; set; } = Topics.RawEvents;
    public string DeltaTopic { get; set; } = Topics.MatchDeltas;

    /// <summary>
    /// Max events the validated-events consumer gathers before a commit barrier. Larger batches
    /// expose more cross-match parallelism (and amortize the offset commit) at the cost of a wider
    /// at-least-once reprocessing window on crash.
    /// </summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>
    /// Upper bound on simultaneously in-flight grain calls while draining a batch. Caps the load
    /// placed on Orleans/Mongo/Redis; per-match ordering is unaffected (same key stays sequential).
    /// </summary>
    public int MaxConcurrency { get; set; } = 256;
}
