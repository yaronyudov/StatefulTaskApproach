using SportsPipeline.Domain;
using Xunit;

namespace SportsPipeline.Tests;

/// <summary>
/// The ±2h window scenario. Anchor is the first processed event (18:00). Events within
/// [16:00, 20:00] inclusive are duplicates (live updates to avoid storing as new games); events
/// outside re-anchor as a new game.
/// </summary>
public class MatchWindowClassifierTests
{
    private const string Key = "arsenal-chelsea-epl-football";
    private static DateTimeOffset At(int hour, int minute = 0) =>
        new(2026, 6, 28, hour, minute, 0, TimeSpan.Zero);

    [Fact]
    public void Scenario_table_classifies_window_correctly()
    {
        var classifier = new MatchWindowClassifier();

        // Start 18:00 (None) -> new game stored (sets the anchor)
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, At(18, 0)));

        // Exact lower bound 16:00 (-2h) -> duplicate
        Assert.Equal(MatchClassification.Duplicate, classifier.Classify(Key, At(16, 0)));

        // Exact upper bound 20:00 (+2h) -> duplicate
        Assert.Equal(MatchClassification.Duplicate, classifier.Classify(Key, At(20, 0)));

        // 17:30 (-30m) -> duplicate
        Assert.Equal(MatchClassification.Duplicate, classifier.Classify(Key, At(17, 30)));

        // 18:30 (+30m) -> duplicate
        Assert.Equal(MatchClassification.Duplicate, classifier.Classify(Key, At(18, 30)));

        // New game 20:01 (+2h1m) -> outside window -> new game stored
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, At(20, 1)));
    }

    [Fact]
    public void Different_match_keys_have_independent_windows()
    {
        var classifier = new MatchWindowClassifier();
        Assert.Equal(MatchClassification.NewGame, classifier.Classify("match-a", At(18, 0)));
        // Same timestamp, different match -> its own first/new game, not a duplicate of match-a.
        Assert.Equal(MatchClassification.NewGame, classifier.Classify("match-b", At(18, 0)));
    }

    [Fact]
    public void New_game_reanchors_so_next_window_is_measured_from_it()
    {
        var classifier = new MatchWindowClassifier();
        classifier.Classify(Key, At(18, 0));            // anchor 18:00
        classifier.Classify(Key, At(20, 1));            // new game, re-anchor 20:01
        // 21:00 is within 2h of the NEW anchor (20:01) -> duplicate
        Assert.Equal(MatchClassification.Duplicate, classifier.Classify(Key, At(21, 0)));
        // 22:02 is > 2h from 20:01 -> new game again
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, At(22, 2)));
    }
}
