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
    /// <summary>Match-status values (from the provider, or computed in mapping) that end a match.</summary>
    private static readonly HashSet<string> EndStatuses = new(StringComparer.OrdinalIgnoreCase) { "OVER", "FINISHED", "FT" };
    /// <summary>Grace TTL left on the live read-model after a match ends, so in-flight readers still see it.</summary>
    private static readonly TimeSpan LiveEvictionGrace = TimeSpan.FromMinutes(10);

    private readonly ILogger<MatchGrain> _logger;
    private readonly IDeltaPublisher _deltaPublisher;
    private readonly ILiveMatchStateStore _liveStore;     // Redis: authoritative live read-model
    private readonly IMatchDetailsStore _archive;         // Mongo: periodic / final archive
    private readonly IMatchSearchStore _search;           // OpenSearch: discovery
    private readonly IConfiguration _configuration;

    private bool _isDirty;                                 // has un-archived (Mongo) state
    private string? _lastDetailsJson;                      // last read-model written (Redis == Mongo content)
    private bool _discoveryIndexed;                         // OpenSearch discovery confirmed (re-derived per activation)
    private IDisposable? _flushTimer;

    public MatchGrain(
        ILogger<MatchGrain> logger,
        IDeltaPublisher deltaPublisher,
        ILiveMatchStateStore liveStore,
        IMatchDetailsStore archive,
        IMatchSearchStore search,
        IConfiguration configuration)
    {
        _logger = logger;
        _deltaPublisher = deltaPublisher;
        _liveStore = liveStore;
        _archive = archive;
        _search = search;
        _configuration = configuration;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var flushIntervalSeconds = _configuration.GetValue<int>("MatchGrain:FlushIntervalSeconds", 10);

        // Write-behind archive: flush the current state to MongoDB on a configured interval instead of
        // per event. Live reads are served from Redis, so this archive lag is invisible to users.
        _flushTimer = this.RegisterGrainTimer(async () =>
        {
            // Self-heal discovery: if the first-event index failed (or we just recovered from a crash),
            // re-assert it. IndexAsync is an idempotent upsert by matchId, so retrying is safe.
            if (!_discoveryIndexed && State.Metadata != null)
            {
                await TryIndexDiscoveryAsync();
            }

            if (_isDirty)
            {
                await FlushToArchiveAsync();
                _isDirty = false;
            }
        }, new() { DueTime = TimeSpan.FromSeconds(flushIntervalSeconds), Period = TimeSpan.FromSeconds(flushIntervalSeconds), Interleave = true });

        return base.OnActivateAsync(cancellationToken);
    }

    public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _flushTimer?.Dispose();

        if (_isDirty)
        {
            await FlushToArchiveAsync();
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

            window = new ActiveMatchWindow
            {
                AnchorStartTime = sportEvent.StartTime,
                SportState = ParseSportState(sportEvent)
            };
            State.ActiveWindows.Add(window);

            // Discovery is written immediately and DIRECTLY to OpenSearch on first sight of a match
            // (no longer via Mongo change streams). Best-effort: if it fails the flush timer retries.
            await TryIndexDiscoveryAsync();

            // Live read-model + durable grain state (Redis). Mongo is archived later by the timer.
            await PersistAsync(window);
            _isDirty = true;
            return;
        }
        else
        {
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

        window.ProviderHighWaterMarks[providerId] = sportEvent.EventTime;

        if (deltas.Count == 0)
        {
            return; // RAM-only update, nothing changed
        }

        // 4. Apply
        window.SportState = newState;

        // 5. PERSIST BEFORE NOTIFY. Writing the live read-model + grain state to Redis BEFORE publishing
        //    the delta guarantees "notified ⇒ readable": a user who reacts to the push reads the value we
        //    already stored. Redis (AOF) survives a pod crash, so there's no stale-store race.
        await PersistAsync(window);
        _isDirty = true; // Mongo archive happens on the flush timer, off the hot path

        // 6. Notify subscribers (SSE fan-out)
        foreach (var delta in deltas)
        {
            await _deltaPublisher.PublishDeltaAsync(sportEvent.MatchKey, delta, default);
        }

        // 7. Match end -> one final archive write, then let the live entry TTL out.
        if (IsMatchOver(newState))
        {
            await FlushToArchiveAsync();
            _isDirty = false;
            await _liveStore.EvictAsync(sportEvent.MatchKey, LiveEvictionGrace);
            DeactivateOnIdle();
        }

        // 8. Cleanup old windows to prevent memory leaks
        State.ActiveWindows.RemoveAll(w => (DateTimeOffset.UtcNow - w.AnchorStartTime).TotalDays > 2);
    }

    /// <summary>Persist the current details to Redis (live read-model) and to durable grain storage.</summary>
    private async Task PersistAsync(ActiveMatchWindow window)
    {
        _lastDetailsJson = BuildDetailsJson(window);
        await _liveStore.WriteAsync(this.GetPrimaryKeyString(), _lastDetailsJson);
        await WriteStateAsync(); // Redis grain storage (configured in Program.cs)
    }

    /// <summary>Write the last known details to the MongoDB archive (the periodic / final flush).</summary>
    private async Task FlushToArchiveAsync()
    {
        if (_lastDetailsJson == null) return;
        await _archive.UpsertAsync(this.GetPrimaryKeyString(), _lastDetailsJson);
    }

    private static bool IsMatchOver(ISportState state) =>
        state is FootballState f && f.MatchStatus != null && EndStatuses.Contains(f.MatchStatus);

    /// <summary>The denormalized read-model returned by the QueryApi details endpoint.</summary>
    private string BuildDetailsJson(ActiveMatchWindow window)
    {
        var details = new
        {
            matchId = this.GetPrimaryKeyString(),
            sportType = State.Metadata?.SportType,
            competitionType = State.Metadata?.CompetitionType,
            teams = State.Metadata?.Teams,
            startTime = window.AnchorStartTime,
            state = window.SportState,
            lastUpdated = DateTimeOffset.UtcNow
        };
        return JsonSerializer.Serialize(details);
    }

    /// <summary>
    /// Best-effort discovery index. Never throws: OpenSearch being briefly down must not drop the event
    /// or the match. On failure the flush timer re-asserts it (IndexAsync is an idempotent upsert by
    /// matchId), which also covers re-indexing after a silo crash resets <see cref="_discoveryIndexed"/>.
    /// </summary>
    private async Task TryIndexDiscoveryAsync()
    {
        try
        {
            await _search.IndexAsync(this.GetPrimaryKeyString(), BuildDiscoveryJson());
            _discoveryIndexed = true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Discovery index failed for {MatchKey}; will retry on next flush", this.GetPrimaryKeyString());
        }
    }

    /// <summary>The minimal discovery document indexed in OpenSearch (find-the-match).</summary>
    private string BuildDiscoveryJson()
    {
        var discovery = new
        {
            matchId = State.Metadata?.MatchId,
            sportType = State.Metadata?.SportType,
            competitionType = State.Metadata?.CompetitionType,
            startTime = State.Metadata?.StartTime,
            teams = State.Metadata?.Teams ?? Array.Empty<string>()
        };
        return JsonSerializer.Serialize(discovery);
    }

    private ISportState ParseSportState(SportEvent sportEvent)
    {
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
