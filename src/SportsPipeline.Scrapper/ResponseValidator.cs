using System.Net.Http.Headers;
using SportsPipeline.Domain;

namespace SportsPipeline.Scrapper;

/// <summary>Raised when a provider response fails a security/validity check and must be discarded.</summary>
public sealed class ValidationException(string message) : Exception(message);

/// <summary>
/// Applies the "good from a security POV" checks to provider responses before anything is trusted or
/// published: bounded size, allowed content-type, and per-event sanity (string length bounds,
/// control-character sanitization, timestamp skew bounds). A formal JSON Schema (e.g. JsonSchema.Net)
/// is the natural drop-in extension point for <see cref="ValidatePayloadSize"/>.
/// </summary>
public sealed class ResponseValidator(ValidationOptions options, TimeProvider timeProvider)
{
    public void ValidateTransport(HttpResponseMessage response, long? contentLength)
    {
        if (contentLength is { } length && length > options.MaxResponseBytes)
        {
            throw new ValidationException($"Response size {length} exceeds limit {options.MaxResponseBytes}.");
        }

        var contentType = response.Content.Headers.ContentType;
        if (contentType is null || !IsAllowed(contentType))
        {
            throw new ValidationException($"Disallowed content-type '{contentType?.MediaType}'.");
        }
    }

    public void ValidatePayloadSize(int eventCount)
    {
        if (eventCount > options.MaxEventsPerResponse)
        {
            throw new ValidationException($"Event count {eventCount} exceeds limit {options.MaxEventsPerResponse}.");
        }
    }

    /// <summary>
    /// Validates and sanitizes a single mapped event. Returns a cleaned copy (control characters
    /// stripped, names trimmed). Throws <see cref="ValidationException"/> when a hard bound is broken.
    /// </summary>
    public SportEvent ValidateEvent(SportEvent ev)
    {
        var now = timeProvider.GetUtcNow();
        var maxSkew = TimeSpan.FromDays(options.MaxTimestampSkewDays);

        if (ev.EventTime < now - maxSkew || ev.EventTime > now + maxSkew)
        {
            throw new ValidationException($"EventTime {ev.EventTime:o} is outside the allowed skew window.");
        }

        return ev with
        {
            SportType = Clean(ev.SportType, nameof(ev.SportType)),
            CompetitionType = Clean(ev.CompetitionType, nameof(ev.CompetitionType)),
            HomeTeam = ev.HomeTeam with
            {
                Name = Clean(ev.HomeTeam.Name, "HomeTeam.Name"),
                Id = Clean(ev.HomeTeam.Id, "HomeTeam.Id"),
            },
            AwayTeam = ev.AwayTeam with
            {
                Name = Clean(ev.AwayTeam.Name, "AwayTeam.Name"),
                Id = Clean(ev.AwayTeam.Id, "AwayTeam.Id"),
            },
        };
    }

    private string Clean(string value, string field)
    {
        if (value.Length > options.MaxStringLength)
        {
            throw new ValidationException($"Field '{field}' exceeds max length {options.MaxStringLength}.");
        }

        // Strip control characters that could corrupt logs, downstream stores, or SSE streams.
        var cleaned = new string(value.Where(c => !char.IsControl(c)).ToArray()).Trim();
        if (cleaned.Length == 0)
        {
            throw new ValidationException($"Field '{field}' is empty after sanitization.");
        }

        return cleaned;
    }

    private bool IsAllowed(MediaTypeHeaderValue contentType) =>
        options.AllowedContentTypes.Any(allowed =>
            string.Equals(allowed, contentType.MediaType, StringComparison.OrdinalIgnoreCase));
}
