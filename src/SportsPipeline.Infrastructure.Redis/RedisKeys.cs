namespace SportsPipeline.Infrastructure.Redis;

/// <summary>Canonical Redis key names so the writer (classifier) and reader (QueryApi) can't drift.</summary>
public static class RedisKeys
{
    /// <summary>Live match-details read-model (authoritative while the match is in progress).</summary>
    public static string LiveMatchDetails(string matchId) => $"SportsPipeline:live:match-details:{matchId}";

    /// <summary>Pub/Sub channel pattern for per-match deltas (SSE fan-out).</summary>
    public static string MatchDeltasChannel(string matchId) => $"match-deltas:{matchId}";
}
