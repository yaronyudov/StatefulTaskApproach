using System.Text.Json;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Files;

/// <summary>
/// Development <see cref="IMappingStore"/> adapter: loads all provider mappings from a single JSON
/// file shaped as { "providerId": { ...ProviderMapping... }, ... }. Loaded once and cached.
/// </summary>
public sealed class JsonFileMappingStore : IMappingStore
{
    private readonly IReadOnlyDictionary<string, ProviderMapping> _mappings;

    private JsonFileMappingStore(IReadOnlyDictionary<string, ProviderMapping> mappings) => _mappings = mappings;

    public static async Task<JsonFileMappingStore> LoadAsync(string filePath, CancellationToken cancellationToken = default)
    {
        await using var stream = File.OpenRead(filePath);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var raw = await JsonSerializer.DeserializeAsync<Dictionary<string, ProviderMapping>>(stream, options, cancellationToken)
            ?? new Dictionary<string, ProviderMapping>();
        return new JsonFileMappingStore(raw);
    }

    public Task<ProviderMapping?> GetAsync(string providerId, CancellationToken cancellationToken = default) =>
        Task.FromResult(_mappings.GetValueOrDefault(providerId));
}
