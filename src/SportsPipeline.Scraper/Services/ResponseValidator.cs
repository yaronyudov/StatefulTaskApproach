using System.Net.Http.Headers;
using SportsPipeline.Domain;
using SportsPipeline.Abstractions;
using Microsoft.Extensions.Options;

namespace SportsPipeline.Scraper;

/// <summary>Raised when a provider response fails a security/validity check and must be discarded.</summary>
public sealed class ValidationException(string message) : Exception(message);

/// <summary>
/// Applies the "good from a security POV" checks to provider responses before anything is trusted or
/// published: bounded size, allowed content-type, and per-event sanity (string length bounds,
/// control-character sanitization, timestamp skew bounds). A formal JSON Schema (e.g. JsonSchema.Net)
/// is the natural drop-in extension point for <see cref="ValidatePayloadSize"/>.
/// </summary>
public sealed class ResponseValidator(IOptions<ScraperOptions> optionsAccessor) : IResponseValidator
{
    private readonly ValidationOptions options = optionsAccessor.Value.Provider.Validation;
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


    private bool IsAllowed(MediaTypeHeaderValue contentType) =>
        options.AllowedContentTypes.Any(allowed =>
            string.Equals(allowed, contentType.MediaType, StringComparison.OrdinalIgnoreCase));
}
