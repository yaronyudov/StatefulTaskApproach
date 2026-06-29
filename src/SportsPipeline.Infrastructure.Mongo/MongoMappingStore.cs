using MongoDB.Driver;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Mongo;

/// <summary>
/// <see cref="IMappingStore"/> adapter: fetches provider mapping rules from MongoDB.
/// Each item should have a <c>ProviderId</c> matching the requested provider.
/// </summary>
public sealed class MongoMappingStore(IMongoCollection<ProviderMapping> collection) : IMappingStore
{
    public async Task<ProviderMapping?> GetAsync(string providerId, CancellationToken cancellationToken = default)
    {
        var filter = Builders<ProviderMapping>.Filter.Eq(x => x.ProviderId, providerId);
        return await collection.Find(filter).FirstOrDefaultAsync(cancellationToken);
    }
}
