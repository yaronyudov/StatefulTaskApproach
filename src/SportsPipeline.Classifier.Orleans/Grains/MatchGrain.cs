using Microsoft.Extensions.Logging;
using Orleans;
using SportsPipeline.Classifier.Orleans.Models;
using SportsPipeline.Domain;
using SportsPipeline.Abstractions;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace SportsPipeline.Classifier.Orleans.Grains;

[GenerateSerializer]
public class ActiveMatchWindow
{
    [Id(0)]
    public DateTimeOffset AnchorStartTime { get; set; }
    [Id(1)]
    public Dictionary<string, DateTimeOffset> ProviderHighWaterMarks { get; set; } = new();
    [Id(2)]
    public ISportState SportState { get; set; } = default!;
}

[GenerateSerializer]
public class MatchMetadata
{
    [Id(0)]
    public string MatchId { get; set; } = string.Empty;
    [Id(1)]
    public string SportType { get; set; } = string.Empty;
    [Id(2)]
    public string CompetitionType { get; set; } = string.Empty;
    [Id(3)]
    public string[] Teams { get; set; } = Array.Empty<string>();
    [Id(4)]
    public DateTimeOffset StartTime { get; set; }
}

[GenerateSerializer]
public class MatchGrainState
{
    [Id(0)]
    public List<ActiveMatchWindow> ActiveWindows { get; set; } = new();
    
    [Id(1)]
    public MatchMetadata? Metadata { get; set; }
}

public class MatchGrain : Grain<MatchGrainState>, IMatchGrain
{
    private readonly ILogger<MatchGrain> _logger;
    private readonly IDeltaPublisher _deltaPublisher;
    private readonly ICacheInvalidator _cacheInvalidator;
    private readonly IConfiguration _configuration;
    private bool _isDirty;
    private IDisposable? _flushTimer;

    public MatchGrain(ILogger<MatchGrain> logger, IDeltaPublisher deltaPublisher, ICacheInvalidator cacheInvalidator, IConfiguration configuration)
    {
        _logger = logger;
        _deltaPublisher = deltaPublisher;
        _cacheInvalidator = cacheInvalidator;
        _configuration = configuration;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var flushIntervalSeconds = _configuration.GetValue<int>("MatchGrain:FlushIntervalSeconds", 10);
        
        // Flush non-crucial dirty state on a configured interval to prevent DB hammering
        _flushTimer = this.RegisterGrainTimer(async () => 
        {
            if (_isDirty)
            {
                await WriteStateAsync();
                _isDirty = false;
            }
        }, new() { DueTime = TimeSpan.FromSeconds(flushIntervalSeconds), Period = TimeSpan.FromSeconds(flushIntervalSeconds), Interleave = true });
        
        return base.OnActivateAsync(cancellationToken);
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_flushTimer != null)
        {
            _flushTimer.Dispose();
        }
        
        if (_isDirty)
        {
            await WriteStateAsync();
            await _cacheInvalidator.InvalidateMatchDetailsAsync(this.GetPrimaryKeyString(), cancellationToken);
            _isDirty = false;
        }
        
        await base.OnDeactivateAsync(reason, cancellationToken);
    }

    public async Task ProcessEventAsync(byte[] eventPayload)
    {
        var sportEvent = SportsPipeline.Contracts.SportEventJson.Deserialize(eventPayload);
        // 1. Window Resolution
        var window = State.ActiveWindows.FirstOrDefault(w => 
            Math.Abs((sportEvent.StartTime - w.AnchorStartTime).TotalHours) <= 2);

        if (window == null)
        {
            _logger.LogInformation("first match found for {MatchKey}", sportEvent.MatchKey);

            // 1. POPULATE METADATA FOR CDC WORKER
            // The CDC worker will read this from MongoDB Change Streams and sync to OpenSearch.
            if (State.Metadata == null)
            {
                State.Metadata = new MatchMetadata 
                {
                    MatchId = sportEvent.MatchKey,
                    SportType = sportEvent.SportType,
                    CompetitionType = sportEvent.CompetitionType,
                    StartTime = sportEvent.StartTime,
                    Teams = new[] { sportEvent.HomeTeam.Name, sportEvent.AwayTeam.Name }
                };
            }

            // 2. MUTATE LOCAL STATE & PERSIST TO MONGO
            window = new ActiveMatchWindow 
            { 
                AnchorStartTime = sportEvent.StartTime,
                SportState = ParseSportState(sportEvent) 
            };
            State.ActiveWindows.Add(window);
            
            // For the first match, we consider everything a "new delta" so we fan it out
            // But we already set SportState above.
            await WriteStateAsync();
            return;
        }
        else
        {
            // For the task -> we should stop here, but as for my own "challenge" I will continue
            _logger.LogInformation("dedup for task found for {MatchKey}", sportEvent.MatchKey);            
        }

        // 2. Idempotency Gate (Clock Skew Safety)
        string providerId = "default";
        if (sportEvent.Metadata.TryGetValue("providerId", out var p))
        {
            providerId = p;
        }

        if (window.ProviderHighWaterMarks.TryGetValue(providerId, out var lastEventTime))
        {
            if (sportEvent.EventTime <= lastEventTime)
            {
                _logger.LogDebug("Ignoring older event from {Provider} for {MatchKey}", providerId, sportEvent.MatchKey);
                return;
            }
        }
        
        // 3. Diffing
        var newState = ParseSportState(sportEvent);
        var deltas = window.SportState.GetChanges(newState);

        // Update RAM State high water mark
        window.ProviderHighWaterMarks[providerId] = sportEvent.EventTime;

        if (deltas.Count == 0)
        {
            // No key changes, just return (RAM updated, no DB hit)
            return;
        }

        // 4. Apply State Update
        window.SportState = newState;

        // 5. Publish to Downstream (SSE Fan-out)
        foreach (var delta in deltas)
        {
            await _deltaPublisher.PublishDeltaAsync(sportEvent.MatchKey, delta, default);
        }

        // 6. DB Flush (Smart Write-Behind)
        // If there are crucial deltas (goals, red cards), write immediately
        if (deltas.Any(d => d.IsCrucial))
        {
            await WriteStateAsync();
            await _cacheInvalidator.InvalidateMatchDetailsAsync(this.GetPrimaryKeyString(), default);
            _isDirty = false;
        }
        // If there are only non-crucial deltas (e.g. yellow cards), just mark as dirty
        // The background timer will flush it soon.
        else if (deltas.Any())
        {
            _isDirty = true;
        }
        
        // 7. Cleanup old windows to prevent memory leaks
        State.ActiveWindows.RemoveAll(w => (DateTimeOffset.UtcNow - w.AnchorStartTime).TotalDays > 2);
    }

    private ISportState ParseSportState(SportEvent sportEvent)
    {
        // We use partial deserialization over the raw JSON if available.
        if (sportEvent.Metadata.TryGetValue(SportsPipeline.Contracts.Constants.EventContextKeys.RawJson, out var rawJson))
        {
            try 
            {
                var state = JsonSerializer.Deserialize<FootballState>(rawJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (state != null) return state;
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to parse rawJson into FootballState");
            }
        }
        
        return new FootballState(0, 0, "Unknown", 0, 0, 0, 0, false, false);
    }
}
