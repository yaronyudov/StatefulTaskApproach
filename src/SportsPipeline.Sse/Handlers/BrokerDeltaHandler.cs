using SportsPipeline.Abstractions;

namespace SportsPipeline.Sse;

/// <summary>
/// Adapts the <see cref="IDeltaHandler"/> port (called by the Kafka delta consumer) to this app's
/// in-process <see cref="DeltaBroker"/>, fanning each delta out to subscribed SSE clients.
/// </summary>
public sealed class BrokerDeltaHandler(DeltaBroker broker) : IDeltaHandler
{
    public void Handle(string matchId, string payload) => broker.Publish(matchId, payload);
}
