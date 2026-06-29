namespace SportsPipeline.QueryApi.Models;

public record MatchSummary(
    string Id, 
    string Sport, 
    string Competition, 
    string HomeTeam, 
    string AwayTeam, 
    DateTimeOffset StartTime);
