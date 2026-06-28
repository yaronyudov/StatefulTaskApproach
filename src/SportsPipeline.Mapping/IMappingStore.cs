namespace SportsPipeline.Mapping;

/// <summary>
/// The mapping database abstraction: returns the translation rules for a given provider. Backed by
/// DynamoDB in AWS (<see cref="DynamoDbMappingStore"/>) and by a local JSON file for development
/// (<see cref="JsonFileMappingStore"/>).
/// </summary>
public interface IMappingStore
{
    Task<ProviderMapping?> GetAsync(string providerId, CancellationToken cancellationToken = default);
}
