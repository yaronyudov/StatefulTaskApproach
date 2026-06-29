using System.Text.Json.Serialization;

namespace SportsPipeline.Classifier.Orleans.Models;

public interface ISportState
{
    IReadOnlyList<MatchDelta> GetChanges(ISportState newState);
}

// Blazing Fast Partial Deserialization:
// The raw JSON contains a ton of data (like livescores.com), but System.Text.Json
// will ONLY parse these specific properties and instantly ignore the rest.
[GenerateSerializer]
public record FootballState(
    [property: Id(0)] int HomeScore,
    [property: Id(1)] int AwayScore,
    [property: Id(2)] string MatchStatus,
    [property: Id(3)] int RedCardsHome,
    [property: Id(4)] int RedCardsAway,
    [property: Id(5)] int YellowCardsHome,
    [property: Id(6)] int YellowCardsAway,
    [property: Id(7)] bool IsPenaltyShootout,
    [property: Id(8)] bool IsVarReviewActive
) : ISportState
{
    public IReadOnlyList<MatchDelta> GetChanges(ISportState newState)
    {
        var changes = new List<MatchDelta>();
        if (newState is not FootballState nf) return changes;

        if (HomeScore != nf.HomeScore) changes.Add(new MatchDelta("HomeScoreChanged", nf.HomeScore, true));
        if (AwayScore != nf.AwayScore) changes.Add(new MatchDelta("AwayScoreChanged", nf.AwayScore, true));
        if (MatchStatus != nf.MatchStatus) changes.Add(new MatchDelta("MatchStatusChanged", nf.MatchStatus, true));
        if (RedCardsHome != nf.RedCardsHome) changes.Add(new MatchDelta("RedCardsHomeChanged", nf.RedCardsHome, true));
        if (RedCardsAway != nf.RedCardsAway) changes.Add(new MatchDelta("RedCardsAwayChanged", nf.RedCardsAway, true));
        
        // Yellow cards are considered "regular" / non-crucial updates that can be flushed lazily
        if (YellowCardsHome != nf.YellowCardsHome) changes.Add(new MatchDelta("YellowCardsHomeChanged", nf.YellowCardsHome, false));
        if (YellowCardsAway != nf.YellowCardsAway) changes.Add(new MatchDelta("YellowCardsAwayChanged", nf.YellowCardsAway, false));
        
        if (IsPenaltyShootout != nf.IsPenaltyShootout) changes.Add(new MatchDelta("IsPenaltyShootoutChanged", nf.IsPenaltyShootout, true));
        if (IsVarReviewActive != nf.IsVarReviewActive) changes.Add(new MatchDelta("IsVarReviewActiveChanged", nf.IsVarReviewActive, true));

        return changes;
    }
}
