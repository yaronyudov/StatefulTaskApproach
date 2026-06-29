using System.Collections.Concurrent;
using System.Threading.Channels;

namespace SportsPipeline.Sse;

/// <summary>
/// In-process fan-out registry: maps a matchId to the set of currently-connected SSE subscribers.
/// Each subscriber gets a bounded channel; <see cref="Publish"/> drops to the slowest consumer's
/// buffer without blocking the Kafka consumer. For multi-instance fan-out, swap this for Redis
/// pub/sub — every instance already consumes the whole topic (its own consumer group), so the only
/// thing Redis would add is a shared live snapshot/TTL cache.
/// </summary>
public sealed class DeltaBroker
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Channel<string>>> _subscribers = new();

    public (Guid Id, ChannelReader<string> Reader) Subscribe(string matchId)
    {
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();
        var group = _subscribers.GetOrAdd(matchId, _ => new ConcurrentDictionary<Guid, Channel<string>>());
        group[id] = channel;
        return (id, channel.Reader);
    }

    public void Unsubscribe(string matchId, Guid id)
    {
        if (_subscribers.TryGetValue(matchId, out var group))
        {
            group.TryRemove(id, out _);
            if (group.IsEmpty)
            {
                _subscribers.TryRemove(matchId, out _);
            }
        }
    }

    public void Publish(string matchId, string payload)
    {
        if (_subscribers.TryGetValue(matchId, out var group))
        {
            foreach (var channel in group.Values)
            {
                channel.Writer.TryWrite(payload);
            }
        }
    }
}
