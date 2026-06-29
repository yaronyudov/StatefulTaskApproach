namespace SportsPipeline.Tests;

public enum MatchClassification
{
    /// <summary>No open window for this match, or the event is outside ±window of the anchor.</summary>
    NewGame,

    /// <summary>Within ±window of the current window's first (anchor) event — a live update.</summary>
    Update,
}

public sealed class MatchWindow
{
    public DateTimeOffset AnchorStartTime { get; }
    public DateTimeOffset MaxEventTime { get; set; }

    public MatchWindow(DateTimeOffset anchorStartTime, DateTimeOffset initialEventTime)
    {
        AnchorStartTime = anchorStartTime;
        MaxEventTime = initialEventTime;
    }
}

/// <summary>
/// Canonical, executable specification of the first-vs-not-first rule, kept in C# so it is unit
/// tested here.
/// </summary>
public sealed class MatchWindowClassifier(TimeSpan? window = null)
{
    private readonly TimeSpan _window = window ?? TimeSpan.FromHours(2);
    private readonly Dictionary<string, List<MatchWindow>> _windows = new();

    public MatchClassification Classify(string matchKey, DateTimeOffset startTime, DateTimeOffset eventTime)
    {
        if (!_windows.TryGetValue(matchKey, out var keyWindows))
        {
            keyWindows = new List<MatchWindow>();
            _windows[matchKey] = keyWindows;
        }

        // Find an active window whose AnchorStartTime is within +/- 2 hours of this event's StartTime
        var matchingWindow = keyWindows.FirstOrDefault(w => Math.Abs((w.AnchorStartTime - startTime).TotalHours) <= _window.TotalHours);

        if (matchingWindow != null)
        {
            // Event belongs to an existing match window.
            // Check if it's an older/late event
            if (eventTime < matchingWindow.MaxEventTime)
            {
                // It's out-of-order/late. Return Update so it's ignored/dropped.
                return MatchClassification.Update;
            }

            // Move the watermark forward
            matchingWindow.MaxEventTime = eventTime;
            return MatchClassification.Update;
        }

        // No matching window found, this is a new game.
        keyWindows.Add(new MatchWindow(startTime, eventTime));
        return MatchClassification.NewGame;
    }
}
