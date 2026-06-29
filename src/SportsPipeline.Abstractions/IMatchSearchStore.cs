using System.Text.Json;

namespace SportsPipeline.Abstractions;

/// <summary>
/// Port: discovery search over the read model. Returns each matching match's raw source document
/// (which includes its <c>matchId</c>). Implemented by the OpenSearch adapter.
/// </summary>
public interface IMatchSearchStore
{
    Task<IReadOnlyList<JsonElement>> SearchAsync(QueryParams query, CancellationToken cancellationToken = default);
    Task IndexAsync(string matchId, string json, CancellationToken cancellationToken = default);
}
