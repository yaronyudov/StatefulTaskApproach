namespace SportsPipeline.QueryApi;

/// <summary>
/// The discovery query parameters. Any combination of 1–4 may be supplied; absent ones are skipped.
/// </summary>
public sealed record QueryParams(
    DateTimeOffset? From = null,
    DateTimeOffset? To = null,
    string? Sport = null,
    string? Competition = null,
    string? Team = null,
    int Size = 50);

/// <summary>
/// Builds the OpenSearch request body for the discovery search as a plain dictionary tree. Kept free
/// of any client type so it is trivially unit-testable: each supplied parameter contributes exactly
/// one <c>filter</c> clause, and the team filter matches the home/away-agnostic <c>teams</c> array.
/// </summary>
public static class MatchQueryBuilder
{
    public static Dictionary<string, object?> BuildSearchBody(QueryParams p)
    {
        var filters = new List<object>();

        if (p.From is not null || p.To is not null)
        {
            var range = new Dictionary<string, object>();
            if (p.From is { } from) range["gte"] = from.UtcDateTime.ToString("o");
            if (p.To is { } to) range["lte"] = to.UtcDateTime.ToString("o");
            filters.Add(new Dictionary<string, object> { ["range"] = new Dictionary<string, object> { ["eventTime"] = range } });
        }

        if (!string.IsNullOrWhiteSpace(p.Sport))
        {
            filters.Add(Term("sport", p.Sport));
        }

        if (!string.IsNullOrWhiteSpace(p.Competition))
        {
            filters.Add(Term("competition", p.Competition));
        }

        if (!string.IsNullOrWhiteSpace(p.Team))
        {
            // 'teams' is indexed as [homeTeam, awayTeam] so a single term matches either side.
            filters.Add(Term("teams", p.Team));
        }

        return new Dictionary<string, object?>
        {
            ["size"] = Math.Clamp(p.Size, 1, 1000),
            ["sort"] = new object[] { new Dictionary<string, object> { ["eventTime"] = new Dictionary<string, object> { ["order"] = "desc" } } },
            ["query"] = new Dictionary<string, object>
            {
                ["bool"] = new Dictionary<string, object> { ["filter"] = filters },
            },
        };
    }

    private static Dictionary<string, object> Term(string field, string value) =>
        new() { ["term"] = new Dictionary<string, object> { [field] = value } };
}
