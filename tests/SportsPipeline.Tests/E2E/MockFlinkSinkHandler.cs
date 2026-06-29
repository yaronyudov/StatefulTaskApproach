using System.Text.Json;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using SportsPipeline.Domain;
using SportsPipeline.Tests;

namespace SportsPipeline.Tests.E2E;

/// <summary>
/// A simulated sink handler that mimics the Flink job's behavior for local E2E testing.
/// </summary>
public sealed class MockFlinkSinkHandler(
    IMatchDetailsStore detailsStore,
    IMatchSearchStore searchStore,
    ILogger<MockFlinkSinkHandler> logger)
{
    private readonly MatchWindowClassifier _classifier = new();

    public async Task HandleAsync(SportEvent sportEvent, CancellationToken cancellationToken)
    {
        var matchKey = sportEvent.MatchKey;
        if (string.IsNullOrEmpty(matchKey))
        {
            logger.LogWarning("Event missing MatchKey. Skipping.");
            return;
        }

        var classification = _classifier.Classify(matchKey, sportEvent.StartTime, sportEvent.EventTime);

        if (classification == MatchClassification.NewGame)
        {
            logger.LogInformation("New Game detected for match {MatchKey}. Writing to databases.", matchKey);
            var json = JsonSerializer.Serialize(sportEvent);
            await detailsStore.UpsertAsync(matchKey, json, cancellationToken);
            await searchStore.IndexAsync(matchKey, json, cancellationToken);
        }
        else
        {
            logger.LogInformation("Duplicate update for match {MatchKey}. Ignoring DB writes.", matchKey);
        }
    }
}
