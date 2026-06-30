using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using OpenSearch.Client;
using StackExchange.Redis;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Mongo;
using SportsPipeline.Infrastructure.OpenSearch;
using SportsPipeline.Infrastructure.Redis;
using SportsPipeline.QueryApi.Models;
using SportsPipeline.QueryApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenSearchMatchSearch(builder.Configuration);

var redisConnStr = builder.Configuration.GetValue<string>("Redis:ConnectionString") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnStr));
builder.Services.AddSingleton<ILiveMatchStateStore, RedisLiveMatchStateStore>();

// Details store = Redis-live first, MongoDB archive on miss. In-progress matches are read from the
// same Redis store they were notified from (always current); ended matches come from MongoDB.
builder.Services.Configure<MongoOptions>(builder.Configuration.GetSection("Mongo"));
builder.Services.AddSingleton<IMatchDetailsStore>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<MongoOptions>>().Value;
    var collection = new MongoClient(opts.ConnectionString)
        .GetDatabase(opts.Database)
        .GetCollection<BsonDocument>(opts.Collection);
    var archive = new MongoMatchDetailsStore(collection);
    var live = sp.GetRequiredService<ILiveMatchStateStore>();
    return new RedisFirstMongoMatchDetailsStore(live, archive);
});

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

// Details: point lookup by matchId (document id).
// Live matches resolve from Redis (the authoritative live store the classifier writes through before
// notifying), so a user who acts on a push always reads the value that triggered it. Ended matches fall
// back to the MongoDB archive. No long-lived caching here: Redis already is the fast, current store.
app.MapGet("/matches/{matchId}", async (string matchId, IMatchDetailsStore details, CancellationToken ct) =>
{
    var json = await details.GetByIdAsync(matchId, ct);
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
