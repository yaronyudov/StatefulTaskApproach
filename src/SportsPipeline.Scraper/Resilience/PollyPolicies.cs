using Polly;
using Polly.Extensions.Http;

namespace SportsPipeline.Scraper;

/// <summary>
/// Builds the HTTP resilience policy for provider calls: retry transient errors and HTTP 429 with
/// exponential backoff + jitter, honoring a server-supplied <c>Retry-After</c> header when present.
/// </summary>
public static class PollyPolicies
{
    public static IAsyncPolicy<HttpResponseMessage> BuildRetryPolicy(BackoffOptions backoff)
    {
        var jitter = new Random();

        return HttpPolicyExtensions
            .HandleTransientHttpError()                          // 5xx + HttpRequestException
            .OrResult(r => (int)r.StatusCode == 429)             // Too Many Requests
            .WaitAndRetryAsync(
                retryCount: backoff.MaxRetries,
                sleepDurationProvider: (attempt, outcome, _) => ComputeDelay(attempt, outcome, backoff, jitter),
                onRetryAsync: (_, _, _, _) => Task.CompletedTask);
    }

    private static TimeSpan ComputeDelay(
        int attempt,
        DelegateResult<HttpResponseMessage> outcome,
        BackoffOptions backoff,
        Random jitter)
    {
        // Prefer the provider's own Retry-After if it gave us one — that is the contract.
        var retryAfter = outcome.Result?.Headers.RetryAfter;
        if (retryAfter is not null)
        {
            if (retryAfter.Delta is { } delta)
            {
                return Cap(delta, backoff);
            }

            if (retryAfter.Date is { } date)
            {
                var until = date - DateTimeOffset.UtcNow;
                if (until > TimeSpan.Zero)
                {
                    return Cap(until, backoff);
                }
            }
        }

        // Otherwise exponential backoff (base * 2^(attempt-1)) with +/- jitter.
        var exponential = backoff.BaseDelayMs * Math.Pow(2, attempt - 1);
        var jitterRange = exponential * backoff.JitterFactor;
        var withJitter = exponential + ((jitter.NextDouble() * 2 - 1) * jitterRange);
        return Cap(TimeSpan.FromMilliseconds(Math.Max(0, withJitter)), backoff);
    }

    private static TimeSpan Cap(TimeSpan value, BackoffOptions backoff) =>
        value > TimeSpan.FromMilliseconds(backoff.MaxDelayMs)
            ? TimeSpan.FromMilliseconds(backoff.MaxDelayMs)
            : value;
}
