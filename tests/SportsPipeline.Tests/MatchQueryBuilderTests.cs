using System.Text.Json;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.OpenSearch;
using Xunit;

namespace SportsPipeline.Tests;

public class MatchQueryBuilderTests
{
    private static JsonElement BuildJson(QueryParams p)
    {
        var body = MatchQueryBuilder.BuildSearchBody(p);
        return JsonSerializer.SerializeToElement(body);
    }

    private static JsonElement Filters(JsonElement root) =>
        root.GetProperty("query").GetProperty("bool").GetProperty("filter");

    [Fact]
    public void No_params_produces_empty_filter_matching_all()
    {
        var filters = Filters(BuildJson(new QueryParams()));
        Assert.Equal(0, filters.GetArrayLength());
    }

    [Fact]
    public void Each_param_adds_exactly_one_filter_clause()
    {
        var p = new QueryParams(
            From: new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero),
            To: new DateTimeOffset(2026, 6, 30, 0, 0, 0, TimeSpan.Zero),
            Sport: "Football",
            Competition: "English Premier League",
            Team: "Arsenal");

        var filters = Filters(BuildJson(p));
        Assert.Equal(4, filters.GetArrayLength()); // time range (1) + sport + competition + team
    }

    [Fact]
    public void Team_filter_targets_order_agnostic_teams_field()
    {
        var json = BuildJson(new QueryParams(Team: "Arsenal"));
        var clause = Filters(json)[0];
        Assert.True(clause.GetProperty("term").TryGetProperty("teams", out var teamValue));
        Assert.Equal("Arsenal", teamValue.GetString());
    }

    [Fact]
    public void Time_only_produces_range_on_event_time()
    {
        var json = BuildJson(new QueryParams(
            From: new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero)));
        var clause = Filters(json)[0];
        Assert.True(clause.GetProperty("range").TryGetProperty("eventTime", out var range));
        Assert.True(range.TryGetProperty("gte", out _));
        Assert.False(range.TryGetProperty("lte", out _)); // no upper bound supplied
    }

    [Fact]
    public void Sport_only_produces_single_term_clause()
    {
        var filters = Filters(BuildJson(new QueryParams(Sport: "Football")));
        Assert.Equal(1, filters.GetArrayLength());
        Assert.Equal("Football", filters[0].GetProperty("term").GetProperty("sport").GetString());
    }
}
