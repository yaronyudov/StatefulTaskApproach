using System.Text.Json;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using SportsPipeline.Abstractions;

namespace SportsPipeline.Scraper;

/// <summary>
/// Fetches the provider's events endpoint, respecting a client-side token-bucket rate limit and (via
/// the injected <see cref="HttpClient"/>'s Polly handler) retry/backoff on transient errors and 429s.
/// Returns the raw JSON elements; mapping and validation happen in the worker.
/// </summary>
public sealed class ProviderPoller : IProviderPoller, IDisposable
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ScraperOptions _options;
    private readonly IResponseValidator _validator;
    private readonly ILogger<ProviderPoller> _logger;
    private readonly TokenBucketRateLimiter _rateLimiter;

    public ProviderPoller(
        IHttpClientFactory httpClientFactory,
        IOptions<ScraperOptions> options,
        IResponseValidator validator,
        ILogger<ProviderPoller> logger)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _validator = validator;
        _logger = logger;
        _rateLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = _options.Provider.RateLimit.Burst,
            TokensPerPeriod = _options.Provider.RateLimit.RequestsPerSecond,
            ReplenishmentPeriod = TimeSpan.FromSeconds(_options.Provider.RateLimit.ReplenishmentPeriod),
            // Place here real production values
            QueueLimit = int.MaxValue,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = true,
        });
    }

    public async IAsyncEnumerable<JsonElement> FetchEventsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Block until the rate limiter grants a token, so we never exceed the provider's limit.
        using var lease = await _rateLimiter.AcquireAsync(1, cancellationToken);
        if (!lease.IsAcquired)
        {
            _logger.LogWarning("Rate limiter denied a lease for provider {ProviderId}.", _options.Provider.Id);
            yield break;
        }

        using var httpClient = _httpClientFactory.CreateClient("provider");
        using var response = await httpClient.GetAsync(_options.Provider.EventsPath, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        _validator.ValidateTransport(response, response.Content.Headers.ContentLength);

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var boundedStream = new BoundedStream(stream, _options.Provider.Validation.MaxResponseBytes);

        var count = 0;
        await foreach (var element in JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(boundedStream, cancellationToken: cancellationToken))
        {
            count++;
            yield return element;
        }

        _validator.ValidatePayloadSize(count);
    }

    private sealed class BoundedStream(Stream inner, long maxBytes) : Stream
    {
        private long _bytesRead;

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await inner.ReadAsync(buffer, cancellationToken);
            _bytesRead += read;
            if (_bytesRead > maxBytes) throw new ValidationException($"Response exceeded the {maxBytes} byte limit.");
            return read;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var read = await inner.ReadAsync(buffer, offset, count, cancellationToken);
            _bytesRead += read;
            if (_bytesRead > maxBytes) throw new ValidationException($"Response exceeded the {maxBytes} byte limit.");
            return read;
        }
        
        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await inner.DisposeAsync();
            await base.DisposeAsync();
        }
    }

    public void Dispose() => _rateLimiter.Dispose();
}
