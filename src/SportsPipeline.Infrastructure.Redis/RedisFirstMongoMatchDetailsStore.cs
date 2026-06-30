using System.Threading;
using System.Threading.Tasks;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Redis;

/// <summary>
/// <see cref="IMatchDetailsStore"/> decorator that reads the LIVE read-model from Redis first and falls
/// back to the MongoDB archive for ended/evicted matches. This makes the read path consistent with the
/// write path: an in-progress match is always served from the same store it was notified from, while
/// finished matches come from durable storage.
/// </summary>
public sealed class RedisFirstMongoMatchDetailsStore : IMatchDetailsStore
{
    private readonly ILiveMatchStateStore _live;
    private readonly IMatchDetailsStore _archive;

    public RedisFirstMongoMatchDetailsStore(ILiveMatchStateStore live, IMatchDetailsStore archive)
    {
        _live = live;
        _archive = archive;
    }

    public async Task<string?> GetByIdAsync(string matchId, CancellationToken cancellationToken = default)
    {
        var live = await _live.ReadAsync(matchId, cancellationToken);
        if (live is not null) return live;

        return await _archive.GetByIdAsync(matchId, cancellationToken);
    }

    public Task UpsertAsync(string matchId, string json, CancellationToken cancellationToken = default)
        => _archive.UpsertAsync(matchId, json, cancellationToken);
}
