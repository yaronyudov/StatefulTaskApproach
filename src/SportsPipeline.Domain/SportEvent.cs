namespace SportsPipeline.Domain;

/// <summary>
/// The internal domain representation of an incoming provider event. Every provider payload is
/// normalized into this shape by the mapping layer before it ever touches Kafka, so the rest of
/// the pipeline is provider-agnostic.
/// </summary>
public sealed record SportEvent
{
    /// <summary>Normalized sport, e.g. "Football".</summary>
    public required string SportType { get; init; }

    /// <summary>Normalized competition, e.g. "English Premier League".</summary>
    public required string CompetitionType { get; init; }

    /// <summary>Scheduled kick-off / start time of the match.</summary>
    public required DateTimeOffset StartTime { get; init; }

    public required Team HomeTeam { get; init; }

    public required Team AwayTeam { get; init; }

    /// <summary>
    /// The timestamp carried by the event itself. This is the value used for the +/-2h
    /// first-vs-not-first windowing decision in the stream processor.
    /// </summary>
    public required DateTimeOffset EventTime { get; init; }

    /// <summary>Free-form provider metadata that survives mapping (kept as strings).</summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } =
        new Dictionary<string, string>();

    /// <summary>The order-agnostic identity key for this match. Stable for home/away swaps.</summary>
    public string MatchKey => MatchIdentity.ComputeKey(SportType, CompetitionType, HomeTeam.Name, AwayTeam.Name);
}
