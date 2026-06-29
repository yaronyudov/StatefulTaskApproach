using System.Text.Json;

namespace SportsPipeline.Abstractions;

public interface IProviderPoller
{
    IAsyncEnumerable<JsonElement> FetchEventsAsync(CancellationToken cancellationToken);
}
