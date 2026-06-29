using SportsPipeline.Domain;
using Xunit;

namespace SportsPipeline.Tests;

public class MatchWindowClassifierTests
{
    private const string Key = "arsenal-chelsea-epl-football";
    private static DateTimeOffset At(int hour, int minute = 0) =>
        new(2026, 6, 28, hour, minute, 0, TimeSpan.Zero);

    [Fact]
    public void Scenario1_SameStartTime_DifferentEventTimes_IsUpdate()
    {
        var classifier = new MatchWindowClassifier();

        // Provider A: Published at 12 PM, Kickoff at 8 PM
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(12, 0)));

        // Provider B: Published at 3 PM, Kickoff at 8 PM
        // EventTime gap is 3 hours, but StartTime is the same, so it should be an Update to the same window!
        Assert.Equal(MatchClassification.Update, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(15, 0)));
    }

    [Fact]
    public void Scenario2_DifferentStartTimes_ConcurrentWindows()
    {
        var classifier = new MatchWindowClassifier();

        // Provider A: Published at 12 PM, Kickoff at 8 PM
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(12, 0)));

        // Provider B: Published at 1:50 PM, Kickoff at 11 PM
        // EventTime gap is < 2h, but StartTime gap is 3h. Should be a NewGame (second active window for same key).
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, startTime: At(23, 0), eventTime: At(13, 50)));

        // Now an update comes for the 8 PM game at 2:00 PM
        Assert.Equal(MatchClassification.Update, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(14, 0)));

        // Now an update comes for the 11 PM game at 4:00 PM
        Assert.Equal(MatchClassification.Update, classifier.Classify(Key, startTime: At(23, 0), eventTime: At(16, 0)));
    }

    [Fact]
    public void Scenario3_LateEvent_IsDroppedAsUpdate()
    {
        var classifier = new MatchWindowClassifier();

        // First event at 12 PM (Kickoff 8 PM)
        Assert.Equal(MatchClassification.NewGame, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(12, 0)));

        // Second event moves watermark to 3 PM
        Assert.Equal(MatchClassification.Update, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(15, 0)));

        // Third event arrives late (EventTime is 1 PM, which is older than MaxEventTime 3 PM)
        // It should be classified as an Update (so it is ignored by MVP sink)
        Assert.Equal(MatchClassification.Update, classifier.Classify(Key, startTime: At(20, 0), eventTime: At(13, 0)));
    }

    [Fact]
    public void Different_match_keys_have_independent_windows()
    {
        var classifier = new MatchWindowClassifier();
        Assert.Equal(MatchClassification.NewGame, classifier.Classify("match-a", At(20, 0), At(12, 0)));
        Assert.Equal(MatchClassification.NewGame, classifier.Classify("match-b", At(20, 0), At(12, 0)));
    }
}
