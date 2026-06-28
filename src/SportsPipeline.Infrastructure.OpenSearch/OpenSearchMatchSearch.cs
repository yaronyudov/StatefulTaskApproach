using System.Text.Json;
using OpenSearch.Client;
using OpenSearch.Net;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.OpenSearch;

/// <summary>
/// <see cref="IMatchSearchStore"/> adapter over the OpenSearch index that Flink populates with
/// first-match rows. Returns each hit's raw <c>_source</c> (which includes the <c>matchId</c>).
/// </summary>
public sealed class OpenSearchMatchSearch(IOpenSearchClient client, string index) : IMatchSearchStore
{
    public async Task<IReadOnlyList<JsonElement>> SearchAsync(QueryParams query, CancellationToken cancellationToken = default)
    {
        var body = MatchQueryBuilder.BuildSearchBody(query);
        var response = await client.LowLevel.SearchAsync<StringResponse>(index, PostData.Serializable(body), ctx: cancellationToken);

        if (!response.Success)
        {
            throw new InvalidOperationException($"OpenSearch query failed: {response.DebugInformation}");
        }

        using var doc = JsonDocument.Parse(response.Body);
        var hits = doc.RootElement.GetProperty("hits").GetProperty("hits");
        return hits.EnumerateArray()
            .Select(h => h.GetProperty("_source").Clone())
            .ToList();
    }
}
