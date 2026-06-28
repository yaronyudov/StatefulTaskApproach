using SportsPipeline.Sse;

var builder = WebApplication.CreateBuilder(args);

var options = builder.Configuration.GetSection("Sse").Get<SseOptions>() ?? new SseOptions();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<DeltaBroker>();
builder.Services.AddHostedService<KafkaDeltaConsumer>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("ok"));

// Server-Sent Events: clients subscribe to a single match and receive each delta as it arrives.
app.MapGet("/subscribe/{matchId}", async (string matchId, DeltaBroker broker, HttpContext ctx, CancellationToken ct) =>
{
    ctx.Response.Headers.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";
    ctx.Response.Headers.Connection = "keep-alive";
    // Stream each event to the client immediately rather than buffering the response.
    ctx.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>()?.DisableBuffering();

    var (id, reader) = broker.Subscribe(matchId);
    try
    {
        // Initial comment so proxies flush headers and the client knows it is connected.
        await ctx.Response.WriteAsync($": subscribed to {matchId}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);

        await foreach (var payload in reader.ReadAllAsync(ct))
        {
            await ctx.Response.WriteAsync($"data: {payload}\n\n", ct);
            await ctx.Response.Body.FlushAsync(ct);
        }
    }
    catch (OperationCanceledException)
    {
        // client disconnected
    }
    finally
    {
        broker.Unsubscribe(matchId, id);
    }
});

app.Run();
