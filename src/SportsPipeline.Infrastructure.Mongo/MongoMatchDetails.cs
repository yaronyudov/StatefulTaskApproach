using MongoDB.Bson;
using MongoDB.Driver;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Mongo;

/// <summary>
/// <see cref="IMatchDetailsStore"/> adapter: fetches match details from MongoDB/Atlas by document id
/// (<c>_id = matchId</c>). Point lookups by id are fast and read-your-write fresh.
/// </summary>
public sealed class MongoMatchDetails(IMongoCollection<BsonDocument> collection) : IMatchDetailsStore
{
    public async Task<string?> GetByIdAsync(string matchId, CancellationToken cancellationToken = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", matchId);
        var doc = await collection.Find(filter).FirstOrDefaultAsync(cancellationToken);
        return doc?.ToJson();
    }
}
