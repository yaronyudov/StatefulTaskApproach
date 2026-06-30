using System;
using System.Threading;
using System.Threading.Tasks;

namespace SportsPipeline.Abstractions;

/// <summary>
/// Port: the authoritative LIVE store for in-progress matches. The classifier writes the current
/// match-details read-model here (write-through) before notifying subscribers, so the store a user is
/// notified from is the same store the app reads from ("notified ⇒ readable"). Backed by Redis; it
/// survives a classifier-pod crash, so live reads never depend on the slower MongoDB archive.
/// </summary>
public interface ILiveMatchStateStore
{
    /// <summary>Write/replace the current details JSON for a live match.</summary>
    Task WriteAsync(string matchId, string detailsJson, CancellationToken cancellationToken = default);

    /// <summary>Read the current live details JSON, or null if the match isn't live (read it from the archive).</summary>
    Task<string?> ReadAsync(string matchId, CancellationToken cancellationToken = default);

    /// <summary>Set a TTL on the live entry once a match ends, so it self-evicts after being archived.</summary>
    Task EvictAsync(string matchId, TimeSpan ttl, CancellationToken cancellationToken = default);
}
