using System.Text.Json;
using SportsPipeline.Mapping;
using Xunit;

namespace SportsPipeline.Tests;

public class ProviderEventMapperTests
{
    private static readonly ProviderMapping Mapping = new()
    {
        ProviderId = "demo",
        SportType = new FieldMap("sport_type", new Dictionary<string, string> { ["SOCCER"] = "Football" }),
        CompetitionType = new FieldMap("competition", new Dictionary<string, string> { ["EPL"] = "English Premier League" }),
        StartTime = new FieldMap("kickoff"),
        EventTime = new FieldMap("ts"),
        HomeTeamName = new FieldMap("home_team.name"),
        HomeTeamId = new FieldMap("home_team.id"),
        AwayTeamName = new FieldMap("away_team.name"),
        AwayTeamId = new FieldMap("away_team.id"),
        MetadataPath = "extra",
    };

    private static JsonElement Payload(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void Maps_fields_and_translates_values()
    {
        var mapper = new ProviderEventMapper();
        var ev = mapper.Map(Payload("""
        {
          "sport_type": "SOCCER",
          "competition": "EPL",
          "kickoff": "2026-06-27T15:00:00Z",
          "ts": "2026-06-27T15:05:00Z",
          "home_team": { "id": "T1", "name": "Arsenal" },
          "away_team": { "id": "T2", "name": "Chelsea" },
          "extra": { "venue": "Emirates" }
        }
        """), Mapping);

        Assert.Equal("Football", ev.SportType);
        Assert.Equal("English Premier League", ev.CompetitionType);
        Assert.Equal("Arsenal", ev.HomeTeam.Name);
        Assert.Equal("T2", ev.AwayTeam.Id);
        Assert.Equal(new DateTimeOffset(2026, 6, 27, 15, 5, 0, TimeSpan.Zero), ev.EventTime);
        Assert.Equal("Emirates", ev.Metadata["venue"]);
    }

    [Fact]
    public void Parses_epoch_millis_timestamps()
    {
        var mapper = new ProviderEventMapper();
        var epoch = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds();
        var ev = mapper.Map(Payload($$"""
        {
          "sport_type": "SOCCER", "competition": "EPL",
          "kickoff": {{epoch}}, "ts": {{epoch}},
          "home_team": { "id": "T1", "name": "A" },
          "away_team": { "id": "T2", "name": "B" }
        }
        """), Mapping);

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(epoch), ev.EventTime);
    }

    [Fact]
    public void Throws_when_required_field_missing()
    {
        var mapper = new ProviderEventMapper();
        Assert.Throws<MappingException>(() => mapper.Map(Payload("""
        { "competition": "EPL", "kickoff": "2026-06-27T15:00:00Z", "ts": "2026-06-27T15:00:00Z",
          "home_team": { "id": "T1", "name": "A" }, "away_team": { "id": "T2", "name": "B" } }
        """), Mapping));
    }

    [Fact]
    public void Unknown_value_passes_through_when_not_in_value_map()
    {
        var mapper = new ProviderEventMapper();
        var ev = mapper.Map(Payload("""
        { "sport_type": "RUGBY", "competition": "EPL", "kickoff": "2026-06-27T15:00:00Z",
          "ts": "2026-06-27T15:00:00Z", "home_team": { "id": "T1", "name": "A" },
          "away_team": { "id": "T2", "name": "B" } }
        """), Mapping);

        Assert.Equal("RUGBY", ev.SportType);
    }
}
