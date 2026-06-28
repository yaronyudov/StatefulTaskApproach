using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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

// Local development uses the JSON mapping file; swap for DynamoDbMappingStore in AWS.
builder.Services.AddSingleton<IMappingStore>(_ =>
    JsonFileMappingStore.LoadAsync(options.MappingFilePath).GetAwaiter().GetResult());

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

builder.Services.AddHostedService<ScrapperWorker>();

var host = builder.Build();
await host.RunAsync();
