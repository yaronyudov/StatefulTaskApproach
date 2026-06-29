namespace SportsPipeline.Abstractions;

/// <summary>
/// Port: fetch full match details by document id. Returns the document as JSON, or null if absent.
/// Implemented by the MongoDB/Atlas adapter.
/// </summary>
public interface IMatchDetailsStore
{
    Task<string?> GetByIdAsync(string matchId, CancellationToken cancellationToken = default);
    Task UpsertAsync(string matchId, string json, CancellationToken cancellationToken = default);
}
