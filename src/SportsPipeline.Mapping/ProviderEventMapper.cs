using System.Globalization;
using System.Text.Json;
using SportsPipeline.Abstractions;
using SportsPipeline.Domain;

namespace SportsPipeline.Mapping;

/// <summary>
/// THIS IS FOR DEMO ONLY TO SHOW I KNOW WE MUST MAP INCOMING DATA INTO DOMAIN DATA
/// Translates a raw provider JSON payload into a domain <see cref="SportEvent"/> using a
/// <see cref="ProviderMapping"/>. Field paths are dotted (e.g. "home_team.name") and values may be
/// remapped via per-field translation dictionaries.
/// </summary>
public sealed class ProviderEventMapper
{
    public SportEvent Map(JsonElement payload, ProviderMapping mapping)
    {
        var home = new Team(
            Id: RequireString(payload, mapping.HomeTeamId),
            Name: RequireString(payload, mapping.HomeTeamName));

        var away = new Team(
            Id: RequireString(payload, mapping.AwayTeamId),
            Name: RequireString(payload, mapping.AwayTeamName));

        return new SportEvent
        {
            SportType = RequireString(payload, mapping.SportType),
            CompetitionType = RequireString(payload, mapping.CompetitionType),
            StartTime = RequireTimestamp(payload, mapping.StartTime),
            EventTime = RequireTimestamp(payload, mapping.EventTime),
            HomeTeam = home,
            AwayTeam = away,
            Metadata = ReadMetadata(payload, mapping.MetadataPath),
        };
    }

    private static string RequireString(JsonElement payload, FieldMap field)
    {
        var raw = ResolvePath(payload, field.SourcePath)
            ?? throw new MappingException($"Required field at path '{field.SourcePath}' was missing.");

        var value = raw.ValueKind switch
        {
            JsonValueKind.String => raw.GetString() ?? string.Empty,
            JsonValueKind.Number => raw.GetRawText(),
            JsonValueKind.True or JsonValueKind.False => raw.GetRawText(),
            _ => throw new MappingException($"Field at path '{field.SourcePath}' is not a scalar."),
        };

        // Apply value translation if configured. Unknown values pass through unchanged so a missing
        // dictionary entry does not silently drop data — surfacing it is the validation layer's job.
        if (field.ValueMap is not null && field.ValueMap.TryGetValue(value, out var translated))
        {
            return translated;
        }

        return value;
    }

    private static DateTimeOffset RequireTimestamp(JsonElement payload, FieldMap field)
    {
        var raw = ResolvePath(payload, field.SourcePath)
            ?? throw new MappingException($"Required timestamp at path '{field.SourcePath}' was missing.");

        // Accept ISO-8601 strings and numeric epoch milliseconds — the two common provider shapes.
        if (raw.ValueKind == JsonValueKind.Number && raw.TryGetInt64(out var epochMillis))
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(epochMillis);
        }

        var text = raw.GetString();
        if (text is not null &&
            DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
        {
            return parsed.ToUniversalTime();
        }

        throw new MappingException($"Field at path '{field.SourcePath}' is not a valid timestamp.");
    }

    private static IReadOnlyDictionary<string, string> ReadMetadata(JsonElement payload, string? metadataPath)
    {
        if (string.IsNullOrWhiteSpace(metadataPath))
        {
            return new Dictionary<string, string>();
        }

        var node = ResolvePath(payload, metadataPath);
        if (node is null || node.Value.ValueKind != JsonValueKind.Object)
        {
            return new Dictionary<string, string>();
        }

        var result = new Dictionary<string, string>();
        foreach (var property in node.Value.EnumerateObject())
        {
            result[property.Name] = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString() ?? string.Empty
                : property.Value.GetRawText();
        }

        return result;
    }

    /// <summary>Walks a dotted path (e.g. "home_team.name") into a JSON object.</summary>
    private static JsonElement? ResolvePath(JsonElement root, string path)
    {
        var current = root;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (current.ValueKind != JsonValueKind.Object ||
                !current.TryGetProperty(segment, out var next))
            {
                return null;
            }

            current = next;
        }

        return current;
    }
}
