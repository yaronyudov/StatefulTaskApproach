using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Kafka;
using SportsPipeline.Infrastructure.Redis;
using SportsPipeline.Sse;
using System;
using System.Threading;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<DeltaBroker>();
builder.Services.AddSingleton<IDeltaHandler, BrokerDeltaHandler>();

var pipelineMode = builder.Configuration.GetValue<string>("PipelineMode") ?? "Orleans";

if (pipelineMode.Equals("Flink", StringComparison.OrdinalIgnoreCase))
{
    // Flink publishes to Kafka match-deltas
    builder.Services.AddKafkaDeltaConsumer(builder.Configuration);
}
else
{
    // Orleans publishes to Redis Pub/Sub
    var redisConnStr = builder.Configuration.GetValue<string>("Redis:ConnectionString") ?? "localhost:6379";
    builder.Services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisConnStr));
    builder.Services.AddHostedService<RedisDeltaConsumer>();
}

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("ok"));

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
