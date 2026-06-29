namespace SportsPipeline.Abstractions;

/// <summary>
/// Abstraction for processing incoming validated events from the scraper infrastructure.
/// </summary>
public interface IValidatedEventProcessor
{
    /// <summary>
    /// Processes a raw event payload (e.g. JSON bytes) from the messaging infrastructure.
    /// </summary>
    /// <param name="rawEventPayload">The exact payload received from the transport (e.g., Kafka).</param>
    Task ProcessEventAsync(byte[] rawEventPayload);
}
