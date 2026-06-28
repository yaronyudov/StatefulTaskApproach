namespace SportsPipeline.Abstractions;

/// <summary>
/// Port: receives a match delta (keyed by matchId) consumed from the stream. The Kafka delta
/// consumer adapter calls this; the SSE app implements it to fan the delta out to subscribers.
/// Decouples the Kafka adapter from the app's subscriber registry.
/// </summary>
public interface IDeltaHandler
{
    void Handle(string matchId, string payload);
}
