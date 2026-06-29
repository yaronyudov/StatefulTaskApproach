using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.OpenSearch;

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
            filters.Add(new Dictionary<string, object> { ["range"] = new Dictionary<string, object> { ["startTime"] = range } });
        }

        // These fields should not be hard coded in here but externally exposed to ensure proper naming will be across entire project -> same goes for ALL over the proect
        // Doing so would require potentially more classes to be used and I want to keep it simple for this task and not blowup with more classes than I already have to ensure this is still readable
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
            ["sort"] = new object[] { new Dictionary<string, object> { ["startTime"] = new Dictionary<string, object> { ["order"] = "desc" } } },
            ["query"] = new Dictionary<string, object>
            {
                ["bool"] = new Dictionary<string, object> { ["filter"] = filters },
            },
        };
    }

    private static Dictionary<string, object> Term(string field, string value) =>
        new() { ["term"] = new Dictionary<string, object> { [field] = value } };
}
