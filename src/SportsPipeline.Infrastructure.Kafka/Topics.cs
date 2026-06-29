namespace SportsPipeline.Infrastructure.Kafka;

/// <summary>
/// Canonical Kafka topic names shared by every service so producers and consumers cannot drift.
/// </summary>
public static class Topics
{
    /// <summary>Raw JSON payload from the provider</summary>
    public const string RawEvents = "raw-events";

    /// <summary>Every validated, domain-mapped event the Validator produces. Keyed by match key.</summary>
    public const string ValidatedEvents = "validated-events";

    /// <summary>First occurrence of a match (no prior event, or outside the +/-2h window).</summary>
    public const string FirstMatchEvents = "first-match-events";

    /// <summary>Subsequent events that fall inside an existing match window (the aggregated stream).</summary>
    public const string NotFirstMatchEvents = "not-first-match-events";

    /// <summary>Every classified event (each update) — consumed by the SSE service to push deltas.</summary>
    public const string MatchDeltas = "match-deltas";
}
