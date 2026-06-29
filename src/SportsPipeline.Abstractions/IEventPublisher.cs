using SportsPipeline.Domain;

namespace SportsPipeline.Abstractions;

/// <summary>Port: publishes a validated domain event to the ingestion stream (Kafka adapter).</summary>
public interface IEventPublisher : IAsyncDisposable
{
    Task PublishAsync(SportEvent ev, CancellationToken cancellationToken);
    Task PublishRawAsync(string topic, string rawPayload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken);
}
