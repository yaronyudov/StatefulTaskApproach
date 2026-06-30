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

    /// <summary>
    /// Number of independent consumer loops to run in the same group within this process. Each loop
    /// is a separate Kafka group member, so the broker assigns it a disjoint set of partitions —
    /// partition-level parallelism on top of the per-key in-batch parallelism. Defaults to 1.
    /// </summary>
    public int ConsumerCount { get; set; } = 1;

    /// <summary>
    /// Desired partition count for the validated-events topic. When &gt; 1 the consumer ensures the
    /// topic exists with this many partitions at startup (create-if-missing). 1 means "leave the
    /// topic as-is" (broker auto-creation). More partitions is what lets multiple consumers/pods
    /// process the stream in parallel while each match stays on a single ordered partition.
    /// </summary>
    public int Partitions { get; set; } = 1;
}
