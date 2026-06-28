using System.Text.Json;
using System.Text.Json.Serialization;
using SportsPipeline.Domain;

namespace SportsPipeline.Contracts;

/// <summary>
/// Single source of truth for how <see cref="SportEvent"/> is serialized on the wire. JSON is used
/// for readability in this implementation; in production this is the natural seam to swap in Avro +
/// a Schema Registry without touching any service logic.
/// </summary>
public static class SportEventJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static byte[] Serialize(SportEvent value) =>
        JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static SportEvent Deserialize(ReadOnlySpan<byte> data) =>
        JsonSerializer.Deserialize<SportEvent>(data, Options)
        ?? throw new JsonException("Payload deserialized to null SportEvent.");
}
