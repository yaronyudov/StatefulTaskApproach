using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace SportsPipeline.Domain;

/// <summary>
/// Computes a stable, order-agnostic identity for a match from its sport, competition and the two
/// team names. Two events for the same fixture must produce the same key regardless of which team
/// is reported as "home" vs "away", and regardless of casing / accents / surrounding whitespace.
///
/// This key is used both as the Kafka partition key (so all events for a fixture land on one
/// partition and are processed in order) and as the keyed-state key in the stream processor.
/// </summary>
public static class MatchIdentity
{
    /// <summary>
    /// Builds the canonical match key: normalize each component, sort the two team names so order
    /// does not matter, join with a separator that cannot appear in a normalized value, then hash.
    /// </summary>
    public static string ComputeKey(string sport, string competition, string homeTeam, string awayTeam)
    {
        var s = Normalize(sport);
        var c = Normalize(competition);
        var t1 = Normalize(homeTeam);
        var t2 = Normalize(awayTeam);

        // Order-agnostic: sort the two normalized team names.
        var (teamA, teamB) = string.CompareOrdinal(t1, t2) <= 0 ? (t1, t2) : (t2, t1);

        var canonical = $"{s}|{c}|{teamA}|{teamB}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Normalize a single component: trim, lower-case (invariant), collapse internal whitespace to
    /// a single space, and strip diacritics so "Bayern München" and "Bayern Munchen" collide.
    /// </summary>
    public static string Normalize(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var stripped = StripDiacritics(value.Trim().ToLowerInvariant());

        // Collapse any run of whitespace to a single space.
        var sb = new StringBuilder(stripped.Length);
        var previousWasSpace = false;
        foreach (var ch in stripped)
        {
            if (char.IsWhiteSpace(ch))
            {
                if (!previousWasSpace)
                {
                    sb.Append(' ');
                    previousWasSpace = true;
                }
            }
            else
            {
                sb.Append(ch);
                previousWasSpace = false;
            }
        }

        return sb.ToString().Trim();
    }

    private static string StripDiacritics(string text)
    {
        var decomposed = text.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(ch) != UnicodeCategory.NonSpacingMark)
            {
                sb.Append(ch);
            }
        }

        return sb.ToString().Normalize(NormalizationForm.FormC);
    }
}
