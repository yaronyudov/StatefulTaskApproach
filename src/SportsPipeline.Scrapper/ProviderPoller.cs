using System.Text.Json;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;

namespace SportsPipeline.Scrapper;

/// <summary>
/// Fetches the provider's events endpoint, respecting a client-side token-bucket rate limit and (via
/// the injected <see cref="HttpClient"/>'s Polly handler) retry/backoff on transient errors and 429s.
/// Returns the raw JSON elements; mapping and validation happen in the worker.
/// </summary>
public sealed class ProviderPoller : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ProviderConfig _config;
    private readonly ResponseValidator _validator;
    private readonly ILogger<ProviderPoller> _logger;
    private readonly TokenBucketRateLimiter _rateLimiter;

    public ProviderPoller(
        HttpClient httpClient,
        ProviderConfig config,
        ResponseValidator validator,
        ILogger<ProviderPoller> logger)
    {
        _httpClient = httpClient;
        _config = config;
        _validator = validator;
        _logger = logger;
        _rateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = config.RateLimit.Burst,
            TokensPerPeriod = config.RateLimit.RequestsPerSecond,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            QueueLimit = int.MaxValue,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = true,
        });
    }

    public async Task<IReadOnlyList<JsonElement>> FetchEventsAsync(CancellationToken cancellationToken)
    {
        // Block until the rate limiter grants a token, so we never exceed the provider's limit.
        using var lease = await _rateLimiter.AcquireAsync(1, cancellationToken);
        if (!lease.IsAcquired)
        {
            _logger.LogWarning("Rate limiter denied a lease for provider {ProviderId}.", _config.Id);
            return [];
        }

        using var response = await _httpClient.GetAsync(_config.EventsPath, cancellationToken);
        response.EnsureSuccessStatusCode();
        _validator.ValidateTransport(response, response.Content.Headers.ContentLength);

        // Enforce the byte cap WHILE reading: a chunked response has no Content-Length, so the
        // transport check above can't catch an oversized (or malicious) body on its own.
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        var bytes = await ReadCappedAsync(stream, _config.Validation.MaxResponseBytes, cancellationToken);
        using var document = JsonDocument.Parse(bytes);

        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            throw new ValidationException("Provider response root is not a JSON array.");
        }

        var events = document.RootElement.EnumerateArray().Select(e => e.Clone()).ToList();
        _validator.ValidatePayloadSize(events.Count);
        return events;
    }

    /// <summary>Reads a stream into memory, aborting if it exceeds <paramref name="maxBytes"/>.</summary>
    private static async Task<byte[]> ReadCappedAsync(Stream stream, long maxBytes, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[81920];
        int read;
        while ((read = await stream.ReadAsync(chunk, cancellationToken)) > 0)
        {
            if (buffer.Length + read > maxBytes)
            {
                throw new ValidationException($"Response exceeded the {maxBytes} byte limit.");
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    public void Dispose() => _rateLimiter.Dispose();
}
