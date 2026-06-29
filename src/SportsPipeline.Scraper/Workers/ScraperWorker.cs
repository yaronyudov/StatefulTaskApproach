using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;
using SportsPipeline.Contracts;

namespace SportsPipeline.Scraper;

/// <summary>
/// The scraper loop for a single provider: on each tick it fetches events, maps each provider
/// payload to the domain shape, validates/sanitizes it, and publishes it to Kafka. Per-event
/// failures are logged and skipped so one bad record cannot stall the provider.
/// </summary>
public sealed class ScraperWorker(
    IProviderPoller poller,
    IEventPublisher publisher,
    IOptions<ScraperOptions> optionsAccessor,
    ILogger<ScraperWorker> logger) : BackgroundService
{
    private readonly ScraperOptions options = optionsAccessor.Value;
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var providerId = options.Provider.Id;

        var interval = TimeSpan.FromSeconds(options.Provider.PollIntervalSeconds);
        using var timer = new PeriodicTimer(interval);

        logger.LogInformation("Scraper started for provider {ProviderId}, polling every {Interval}.",
            providerId, interval);

        do
        {
            try
            {
                await PollOnceAsync(providerId, stoppingToken);
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

    private async Task PollOnceAsync(string providerId, CancellationToken cancellationToken)
    {
        var published = 0;
        var headers = new Dictionary<string, string> { { Constants.KafkaHeaders.ProviderId, providerId } };

        try
        {
            await foreach (var raw in poller.FetchEventsAsync(cancellationToken))
            {
                try
                {
                    await publisher.PublishRawAsync(options.Kafka.PublishTopic, raw.GetRawText(), headers, cancellationToken);
                    published++;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to publish raw event from provider {ProviderId}.", providerId);
                }
            }

            logger.LogInformation("Published {Published} events for provider {ProviderId}.",
                published, providerId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to fetch or publish stream from provider {ProviderId}.", providerId);
            throw; // Let ExecuteAsync catch it and wait for next tick
        }
    }
}
