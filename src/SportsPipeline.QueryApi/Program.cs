using MongoDB.Bson;
using MongoDB.Driver;
using OpenSearch.Client;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Mongo;
using SportsPipeline.Infrastructure.OpenSearch;
using SportsPipeline.QueryApi.Models;
using SportsPipeline.QueryApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenSearchMatchSearch(builder.Configuration);
builder.Services.AddMongoMatchDetailsStore(builder.Configuration);

var redisConnStr = builder.Configuration.GetValue<string>("Redis:ConnectionString") ?? "localhost:6379";
builder.Services.AddStackExchangeRedisCache(options => 
{
    options.Configuration = redisConnStr;
    options.InstanceName = "SportsPipeline:";
});
builder.Services.AddSingleton<CacheStampedeProtector>();

var app = builder.Build();

// *****  This below is for simplicity, I would push this to a proper strucutre of controllers and services and so on 
app.MapGet("/health", () => Results.Ok("ok"));

// Discovery: search by any combination of 1-4 params, return match summaries + matchId.
app.MapGet("/matches", async (
    DateTimeOffset? from, DateTimeOffset? to, string? sport, string? competition, string? team, int? size,
    IMatchSearchStore search, CancellationToken ct) =>
{   
    if (sport is null && competition is null && team is null && from is null && to is null)
    {
        return Results.BadRequest("At least one parameter is expected");
    }
    // Again - for simplicity only - log "abuse" detected
    if (size < 1 || size > 100) size = 50;
    
    var p = new QueryParams(from, to, sport, competition, team, size ?? 50);
    var hits = await search.SearchAsync(p, ct);
    var summaries = hits.Select(MapToSummary);
    return Results.Json(summaries);
});

// -> not for this scope but AI already implemented
// Details: point lookup by matchId (document id). 
app.MapGet("/matches/{matchId}", async (string matchId, IMatchDetailsStore details, CacheStampedeProtector cache, CancellationToken ct) =>
{
    // Caching Strategy:
    // We cache the result in Redis. For non-live matches, this effectively lives forever (or a very long TTL).
    // For live matches, Orleans will actively INVALIDATE this cache key when a crucial stat (goal/card) occurs.
    var cacheKey = $"match-details:{matchId}";
    
    // We use a 24-hour TTL by default, assuming matches end within that time or get updated.
    var json = await cache.GetOrAddAsync(cacheKey, 
        () => details.GetByIdAsync(matchId, ct), 
        TimeSpan.FromHours(24), 
        ct);

    return json is null ? Results.NotFound() : Results.Content(json, "application/json");
});

app.Run();

MatchSummary MapToSummary(System.Text.Json.JsonElement h)
{
    var id = h.TryGetProperty("matchId", out var idProp) ? idProp.GetString() : h.GetProperty("MatchKey").GetString();
    var sport = h.TryGetProperty("sport", out var sProp) ? sProp.GetString() : h.GetProperty("SportType").GetString();
    var comp = h.TryGetProperty("competition", out var cProp) ? cProp.GetString() : h.GetProperty("CompetitionType").GetString();
    
    var home = h.TryGetProperty("homeTeam", out var hProp) && hProp.ValueKind == System.Text.Json.JsonValueKind.String 
        ? hProp.GetString() 
        : (h.TryGetProperty("HomeTeam", out var htObj) ? htObj.GetProperty("Name").GetString() : h.GetProperty("homeTeam").GetProperty("name").GetString());
        
    var away = h.TryGetProperty("awayTeam", out var aProp) && aProp.ValueKind == System.Text.Json.JsonValueKind.String 
        ? aProp.GetString() 
        : (h.TryGetProperty("AwayTeam", out var atObj) ? atObj.GetProperty("Name").GetString() : h.GetProperty("awayTeam").GetProperty("name").GetString());
        
    var start = h.TryGetProperty("startTime", out var stProp) ? stProp.GetDateTimeOffset() : h.GetProperty("StartTime").GetDateTimeOffset();
    
    return new MatchSummary(id ?? "", sport ?? "", comp ?? "", home ?? "", away ?? "", start);
}
