namespace SportsPipeline.Abstractions;

/// <summary>
/// Port: returns the provider→domain translation rules for a provider. Implemented by DynamoDB
/// (production) and a local JSON file (development) adapters in the Infrastructure.* projects.
/// </summary>
public interface IMappingStore
{
    Task<ProviderMapping?> GetAsync(string providerId, CancellationToken cancellationToken = default);
}
