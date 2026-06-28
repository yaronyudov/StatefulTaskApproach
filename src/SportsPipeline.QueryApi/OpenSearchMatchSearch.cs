using System.Text.Json;
using OpenSearch.Client;
using OpenSearch.Net;

namespace SportsPipeline.QueryApi;

/// <summary>
/// Executes the discovery search against the OpenSearch index that Flink populates with first-match
/// rows. Returns each hit's raw <c>_source</c> JSON (which includes the <c>matchId</c>); the caller
/// uses that id to fetch full details from MongoDB.
/// </summary>
public sealed class OpenSearchMatchSearch(IOpenSearchClient client, string index)
{
    public async Task<IReadOnlyList<JsonElement>> SearchAsync(QueryParams p, CancellationToken ct)
    {
        var body = MatchQueryBuilder.BuildSearchBody(p);
        var response = await client.LowLevel.SearchAsync<StringResponse>(index, PostData.Serializable(body), ctx: ct);

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
