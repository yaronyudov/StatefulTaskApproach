using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using SportsPipeline.Mapping;

namespace SportsPipeline.Scrapper;

/// <summary>
/// The scrapper loop for a single provider: on each tick it fetches events, maps each provider
/// payload to the domain shape, validates/sanitizes it, and publishes it to Kafka. Per-event
/// failures are logged and skipped so one bad record cannot stall the provider.
/// </summary>
public sealed class ScrapperWorker(
    ProviderPoller poller,
    IEventPublisher publisher,
    IMappingStore mappingStore,
    ProviderEventMapper mapper,
    ResponseValidator validator,
    ScrapperOptions options,
    ILogger<ScrapperWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var providerId = options.Provider.Id;
        var mapping = await mappingStore.GetAsync(providerId, stoppingToken)
            ?? throw new InvalidOperationException($"No mapping configured for provider '{providerId}'.");

        var interval = TimeSpan.FromSeconds(options.Provider.PollIntervalSeconds);
        using var timer = new PeriodicTimer(interval);

        logger.LogInformation("Scrapper started for provider {ProviderId}, polling every {Interval}.",
            providerId, interval);

        do
        {
            try
            {
                await PollOnceAsync(mapping, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Whole-poll failures (network exhausted retries, transport validation) are logged and
                // retried on the next tick rather than crashing the worker.
                logger.LogError(ex, "Poll cycle failed for provider {ProviderId}.", providerId);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    private async Task PollOnceAsync(ProviderMapping mapping, CancellationToken cancellationToken)
    {
        var rawEvents = await poller.FetchEventsAsync(cancellationToken);
        var published = 0;

        foreach (var raw in rawEvents)
        {
            try
            {
                var mapped = mapper.Map(raw, mapping);
                var validated = validator.ValidateEvent(mapped);
                await publisher.PublishAsync(validated, cancellationToken);
                published++;
            }
            catch (Exception ex) when (ex is MappingException or ValidationException)
            {
                logger.LogWarning(ex, "Dropping invalid event from provider {ProviderId}.", mapping.ProviderId);
            }
        }

        logger.LogInformation("Published {Published}/{Total} events for provider {ProviderId}.",
            published, rawEvents.Count, mapping.ProviderId);
    }
}
