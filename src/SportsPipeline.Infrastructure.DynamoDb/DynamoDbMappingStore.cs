using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.DynamoDb;

/// <summary>
/// Production <see cref="IMappingStore"/> adapter. Each item is keyed by <c>providerId</c> and carries
/// a single <c>mappingJson</c> attribute containing the serialized <see cref="ProviderMapping"/>.
/// Storing the rules as a JSON blob keeps the table schema-stable and reads to a single GetItem.
/// </summary>
public sealed class DynamoDbMappingStore(IAmazonDynamoDB client, string tableName) : IMappingStore
{
    private const string KeyAttribute = "providerId";
    private const string MappingAttribute = "mappingJson";

    private readonly JsonSerializerOptions _options = new(JsonSerializerDefaults.Web);

    public async Task<ProviderMapping?> GetAsync(string providerId, CancellationToken cancellationToken = default)
    {
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue> { [KeyAttribute] = new() { S = providerId } },
            ConsistentRead = true,
        }, cancellationToken);

        if (!response.IsItemSet || !response.Item.TryGetValue(MappingAttribute, out var attribute))
        {
            return null;
        }

        return JsonSerializer.Deserialize<ProviderMapping>(attribute.S, _options);
    }
}
