namespace SportsPipeline.Abstractions;

/// <summary>
/// Discovery query parameters. Any combination of 1–4 may be supplied; absent ones are skipped.
/// Part of the <see cref="IMatchSearchStore"/> port contract.
/// </summary>
public sealed record QueryParams(
    DateTimeOffset? From = null,
    DateTimeOffset? To = null,
    string? Sport = null,
    string? Competition = null,
    string? Team = null,
    int Size = 50);
