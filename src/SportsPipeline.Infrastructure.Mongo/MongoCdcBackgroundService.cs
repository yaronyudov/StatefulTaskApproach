using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Mongo;

public class MongoCdcBackgroundService : BackgroundService
{
    private readonly IMongoClient _mongoClient;
    private readonly IMatchSearchStore _searchStore;
    private readonly ILogger<MongoCdcBackgroundService> _logger;
    private readonly string _databaseName;

    public MongoCdcBackgroundService(IOptions<MongoOptions> options, IMatchSearchStore searchStore, ILogger<MongoCdcBackgroundService> logger)
    {
        _mongoClient = new MongoClient(options.Value.ConnectionString);
        _databaseName = options.Value.Database;
        _searchStore = searchStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Mongo CDC worker...");

        var database = _mongoClient.GetDatabase(_databaseName);
        
        // Watch the database for inserts and updates
        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .Match(c => c.OperationType == ChangeStreamOperationType.Insert || c.OperationType == ChangeStreamOperationType.Update || c.OperationType == ChangeStreamOperationType.Replace);

        var options = new ChangeStreamOptions { FullDocument = ChangeStreamFullDocumentOption.UpdateLookup };

        try
        {
            // Watch the entire database in this architectural demo
            using var cursor = database.Watch(pipeline, options, cancellationToken: stoppingToken);
            
            while (await cursor.MoveNextAsync(stoppingToken))
            {
                foreach (var change in cursor.Current)
                {
                    await ProcessChangeAsync(change);
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Mongo CDC worker stopped.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Mongo CDC worker");
        }
    }

    private async Task ProcessChangeAsync(ChangeStreamDocument<BsonDocument> change)
    {
        if (change.FullDocument == null) return;
        
        try
        {
            // Extract Grain State payload. Orleans MongoDB provider typically stores it in a "State" field.
            if (!change.FullDocument.TryGetValue("State", out var stateObj) || !stateObj.IsBsonDocument)
                return;

            var stateDoc = stateObj.AsBsonDocument;

            // Extract the Metadata field we added
            if (!stateDoc.TryGetValue("Metadata", out var metadataObj) || !metadataObj.IsBsonDocument || metadataObj.IsBsonNull)
                return;

            var metadataDoc = metadataObj.AsBsonDocument;

            string matchId = metadataDoc.GetValue("MatchId", "").AsString;
            if (string.IsNullOrEmpty(matchId)) return;

            var discoveryDoc = new 
            {
                matchId = matchId,
                sportType = metadataDoc.GetValue("SportType", "").AsString,
                competitionType = metadataDoc.GetValue("CompetitionType", "").AsString,
                startTime = metadataDoc.Contains("StartTime") && !metadataDoc["StartTime"].IsBsonNull 
                                ? metadataDoc["StartTime"].ToUniversalTime() 
                                : DateTime.UtcNow,
                teams = metadataDoc.Contains("Teams") && metadataDoc["Teams"].IsBsonArray 
                    ? metadataDoc["Teams"].AsBsonArray.Select(x => x.AsString).ToArray() 
                    : Array.Empty<string>()
            };

            var json = JsonSerializer.Serialize(discoveryDoc);
            
            // Sync to OpenSearch
            await _searchStore.IndexAsync(matchId, json);
            _logger.LogDebug("CDC Sync complete for Match {MatchId}", matchId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse CDC document to update OpenSearch");
        }
    }
}
