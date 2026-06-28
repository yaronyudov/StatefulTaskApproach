namespace SportsPipeline.Domain;

public enum MatchClassification
{
    /// <summary>No open window for this match, or the event is outside ±window of the anchor.</summary>
    NewGame,

    /// <summary>Within ±window of the current window's first (anchor) event — a live update.</summary>
    Duplicate,
}

/// <summary>
/// Canonical, executable specification of the first-vs-not-first rule, kept in C# so it is unit
/// tested here. The production Flink job (<c>flink/java/.../FirstMatchClassifier.java</c>) implements
/// the identical logic with keyed state; the shipped Flink SQL uses a relaxed approximation.
///
/// <para>Per match key, the FIRST processed event sets an anchor. Any later event whose timestamp is
/// within <c>[anchor - window, anchor + window]</c> (inclusive) is a <see cref="MatchClassification.Duplicate"/>
/// (a live update to be aggregated, NOT a new game). An event outside that window is a
/// <see cref="MatchClassification.NewGame"/> and re-anchors the window. Order-agnostic identity is
/// already handled upstream by <see cref="MatchIdentity"/>, so this works on the match key.</para>
/// </summary>
public sealed class MatchWindowClassifier(TimeSpan? window = null)
{
    private readonly TimeSpan _window = window ?? TimeSpan.FromHours(2);
    private readonly Dictionary<string, DateTimeOffset> _anchors = new();

    public MatchClassification Classify(string matchKey, DateTimeOffset eventTime)
    {
        if (_anchors.TryGetValue(matchKey, out var anchor) &&
            eventTime >= anchor - _window && eventTime <= anchor + _window)
        {
            return MatchClassification.Duplicate;
        }

        // First event for the key, or outside the window -> a new game; (re)anchor here.
        _anchors[matchKey] = eventTime;
        return MatchClassification.NewGame;
    }
}
