using SportsPipeline.Domain;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Validator;

/// <summary>Raised when a domain event fails validity checks.</summary>
public sealed class ValidationException(string message) : Exception(message);

public sealed class DomainEventValidator(TimeProvider timeProvider) : IDomainEventValidator
{
    /// <summary>
    /// Validates and sanitizes a single mapped event. Returns a cleaned copy (control characters
    /// stripped, names trimmed). Throws <see cref="ValidationException"/> when a hard bound is broken.
    /// </summary>
    public SportEvent ValidateEvent(SportEvent ev)
    {
        var now = timeProvider.GetUtcNow();
        var maxSkew = TimeSpan.FromDays(2); // Hardcoded for MVP, was in ValidationOptions

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
        if (value.Length > 200) // Hardcoded max length for MVP
        {
            throw new ValidationException($"Field '{field}' exceeds max length 200.");
        }

        var cleaned = new string(value.Where(c => !char.IsControl(c)).ToArray()).Trim();
        if (cleaned.Length == 0)
        {
            throw new ValidationException($"Field '{field}' is empty after sanitization.");
        }

        return cleaned;
    }
}
