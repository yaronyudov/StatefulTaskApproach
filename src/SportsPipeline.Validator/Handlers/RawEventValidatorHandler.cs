using System.Text.Json;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using SportsPipeline.Validator.Mapping;

namespace SportsPipeline.Validator;

public sealed class RawEventValidatorHandler(
    IMappingStore mappingStore,
    IProviderEventMapper mapper,
    IDomainEventValidator validator,
    IEventPublisher publisher,
    ILogger<RawEventValidatorHandler> logger) : IRawEventHandler
{
    public async Task HandleAsync(string providerId, string rawJson, CancellationToken cancellationToken)
    {
        var mapping = await mappingStore.GetAsync(providerId, cancellationToken);
        if (mapping is null)
        {
            logger.LogWarning("No mapping found for provider {ProviderId}", providerId);
            return;
        }

        // Parse the raw json directly from the message value
        var rawJsonDoc = JsonDocument.Parse(rawJson);
        var mappedEvent = mapper.Map(rawJsonDoc.RootElement, mapping);

        // Ensure Metadata dictionary exists and inject infrastructure context (Kafka Headers)
        // This guarantees that downstream stateful processors (Orleans/Flink) have access to the raw payload for partial deserialization
        var metadata = new Dictionary<string, string>(mappedEvent.Metadata);
        metadata[SportsPipeline.Contracts.Constants.EventContextKeys.ProviderId] = providerId;
        metadata[SportsPipeline.Contracts.Constants.EventContextKeys.RawJson] = rawJson;
        
        mappedEvent = mappedEvent with { Metadata = metadata };

        var validatedEvent = validator.ValidateEvent(mappedEvent);

        // Publish to validated-events topic
        await publisher.PublishAsync(validatedEvent, cancellationToken);
    }
}
