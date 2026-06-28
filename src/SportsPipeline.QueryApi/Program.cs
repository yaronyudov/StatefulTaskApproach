using MongoDB.Bson;
using MongoDB.Driver;
using OpenSearch.Client;
using SportsPipeline.QueryApi;

var builder = WebApplication.CreateBuilder(args);

var osUrl = builder.Configuration["OpenSearch:Url"] ?? "http://localhost:9200";
var osIndex = builder.Configuration["OpenSearch:Index"] ?? "matches";
var mongoConn = builder.Configuration["Mongo:ConnectionString"] ?? "mongodb://localhost:27017";
var mongoDb = builder.Configuration["Mongo:Database"] ?? "sports";
var mongoCollection = builder.Configuration["Mongo:Collection"] ?? "matches";

var osSettings = new ConnectionSettings(new Uri(osUrl)).DefaultIndex(osIndex);
builder.Services.AddSingleton<IOpenSearchClient>(new OpenSearchClient(osSettings));
builder.Services.AddSingleton(sp => new OpenSearchMatchSearch(sp.GetRequiredService<IOpenSearchClient>(), osIndex));

var mongo = new MongoClient(mongoConn).GetDatabase(mongoDb).GetCollection<BsonDocument>(mongoCollection);
builder.Services.AddSingleton(mongo);
builder.Services.AddSingleton(sp => new MongoMatchDetails(sp.GetRequiredService<IMongoCollection<BsonDocument>>()));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("ok"));

// Discovery: search OpenSearch by any combination of 1-4 params, return match summaries + matchId.
app.MapGet("/matches", async (
    DateTimeOffset? from, DateTimeOffset? to, string? sport, string? competition, string? team, int? size,
    OpenSearchMatchSearch search, CancellationToken ct) =>
{
    var p = new QueryParams(from, to, sport, competition, team, size ?? 50);
    var hits = await search.SearchAsync(p, ct);
    // Return the raw _source documents (each carries matchId for the details lookup).
    return Results.Json(hits);
});

// Details: point lookup in MongoDB by matchId (document id).
app.MapGet("/matches/{matchId}", async (string matchId, MongoMatchDetails details, CancellationToken ct) =>
{
    var json = await details.GetByIdAsync(matchId, ct);
    return json is null ? Results.NotFound() : Results.Content(json, "application/json");
});

app.Run();
