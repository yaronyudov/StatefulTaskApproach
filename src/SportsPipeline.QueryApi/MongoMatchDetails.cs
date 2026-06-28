using MongoDB.Bson;
using MongoDB.Driver;

namespace SportsPipeline.QueryApi;

/// <summary>
/// Fetches full match details from MongoDB by document id (<c>_id = matchId</c>). MongoDB is the
/// system of record that Flink keeps current from the delta stream; point lookups by id are fast and
/// read-your-write fresh.
/// </summary>
public sealed class MongoMatchDetails(IMongoCollection<BsonDocument> collection)
{
    public async Task<string?> GetByIdAsync(string matchId, CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", matchId);
        var doc = await collection.Find(filter).FirstOrDefaultAsync(ct);
        return doc?.ToJson();
    }
}
