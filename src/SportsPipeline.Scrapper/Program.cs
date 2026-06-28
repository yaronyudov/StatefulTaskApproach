using Amazon.DynamoDBv2;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.DynamoDb;
using SportsPipeline.Infrastructure.Files;
using SportsPipeline.Infrastructure.Kafka;
using SportsPipeline.Mapping;
using SportsPipeline.Scrapper;

var builder = Host.CreateApplicationBuilder(args);

var options = builder.Configuration.GetSection("Scrapper").Get<ScrapperOptions>() ?? new ScrapperOptions();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(options.Provider);
builder.Services.AddSingleton(options.Kafka);
builder.Services.AddSingleton(options.Provider.Validation);
builder.Services.AddSingleton(TimeProvider.System);

builder.Services.AddSingleton<ResponseValidator>();
builder.Services.AddSingleton<ProviderEventMapper>();

// Composition root: pick the mapping-store adapter by config ("file" for local, "dynamodb" for AWS).
builder.Services.AddSingleton<IMappingStore>(_ => options.MappingStore.ToLowerInvariant() switch
{
    "dynamodb" => new DynamoDbMappingStore(new AmazonDynamoDBClient(), options.MappingTableName),
    _ => JsonFileMappingStore.LoadAsync(options.MappingFilePath).GetAwaiter().GetResult(),
});

builder.Services.AddSingleton<IEventPublisher>(_ => new KafkaEventPublisher(options.Kafka));

// HttpClient with the Polly retry/backoff policy applied to every provider request.
builder.Services
    .AddHttpClient("provider", client =>
    {
        client.BaseAddress = new Uri(options.Provider.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(30);
    })
    .AddPolicyHandler(PollyPolicies.BuildRetryPolicy(options.Provider.Backoff));

builder.Services.AddSingleton<ProviderPoller>(sp =>
{
    var factory = sp.GetRequiredService<IHttpClientFactory>();
    return new ProviderPoller(
        factory.CreateClient("provider"),
        options.Provider,
        sp.GetRequiredService<ResponseValidator>(),
        sp.GetRequiredService<ILogger<ProviderPoller>>());
});

// for simplicity this is a single worker 
// -> I would set a new hosted service per provider if we had multiple providers to poll in parallel 
// Why? so we can "block" one without affecting the others and ensuring "noisy neighbour" issue won't occur 
// Potentially even use a pod per provider if needed (overcomplexing IMO for this task but MUST be considered for production workload)  
builder.Services.AddHostedService<ScrapperWorker>();

var host = builder.Build();
await host.RunAsync();
