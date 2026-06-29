namespace SportsPipeline.Abstractions;

/// <summary>
/// Maps a single domain field to a path in the provider payload, with an optional value-translation
/// dictionary (e.g. provider "SOCCER" -> domain "Football"). <see cref="SourcePath"/> is a dotted
/// path into the provider JSON, e.g. "home_team.name".
/// </summary>
public sealed record FieldMap(string SourcePath, IReadOnlyDictionary<string, string>? ValueMap = null);

/// <summary>
/// The full set of rules for translating one provider's payload into a domain event. This is the
/// record returned by the mapping store, one per provider. It is part of the port contract, so it
/// lives in Abstractions (not in any concrete adapter).
/// </summary>
public sealed record ProviderMapping
{
    public required string ProviderId { get; init; }

    public required FieldMap SportType { get; init; }
    public required FieldMap CompetitionType { get; init; }
    public required FieldMap StartTime { get; init; }
    public required FieldMap EventTime { get; init; }

    public required FieldMap HomeTeamName { get; init; }
    public required FieldMap HomeTeamId { get; init; }
    public required FieldMap AwayTeamName { get; init; }
    public required FieldMap AwayTeamId { get; init; }

    /// <summary>Optional path to an object whose properties are copied verbatim into metadata.</summary>
    public string? MetadataPath { get; init; }
}
