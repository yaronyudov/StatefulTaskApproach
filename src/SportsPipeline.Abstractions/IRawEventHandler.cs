namespace SportsPipeline.Abstractions;

/// <summary>
/// App-level handler for a raw JSON event consumed from Kafka.
/// </summary>
public interface IRawEventHandler
{
    Task HandleAsync(string providerId, string rawJson, CancellationToken cancellationToken);
}
