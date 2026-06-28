using MongoDB.Bson;
using MongoDB.Driver;
using OpenSearch.Client;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Mongo;
using SportsPipeline.Infrastructure.OpenSearch;

var builder = WebApplication.CreateBuilder(args);

 // ***** For simplicity, must be pulled from env var 
var osUrl = builder.Configuration["OpenSearch:Url"] ?? "http://localhost:9200";
var osIndex = builder.Configuration["OpenSearch:Index"] ?? "matches";
var mongoConn = builder.Configuration["Mongo:ConnectionString"] ?? "mongodb://localhost:27017";
var mongoDb = builder.Configuration["Mongo:Database"] ?? "sports";
var mongoCollection = builder.Configuration["Mongo:Collection"] ?? "matches";

// Composition root: the only place that knows the concrete adapters. The endpoints below depend
// solely on the IMatchSearchStore / IMatchDetailsStore ports.
var osSettings = new ConnectionSettings(new Uri(osUrl)).DefaultIndex(osIndex);
var osClient = new OpenSearchClient(osSettings);
builder.Services.AddSingleton<IMatchSearchStore>(new OpenSearchMatchSearch(osClient, osIndex));

var mongo = new MongoClient(mongoConn).GetDatabase(mongoDb).GetCollection<BsonDocument>(mongoCollection);
builder.Services.AddSingleton<IMatchDetailsStore>(new MongoMatchDetails(mongo));

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
    return Results.Json(hits);
});

// -> not for this scope but AI already implemented
// Details: point lookup by matchId (document id). 
app.MapGet("/matches/{matchId}", async (string matchId, IMatchDetailsStore details, CancellationToken ct) =>
{
    var json = await details.GetByIdAsync(matchId, ct);
    return json is null ? Results.NotFound() : Results.Content(json, "application/json");
});

app.Run();
