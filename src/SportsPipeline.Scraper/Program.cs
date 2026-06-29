using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Kafka;
using SportsPipeline.Scraper;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<ScraperOptions>(builder.Configuration.GetSection("Scraper"));

// We still need the config to construct the composition root singletons manually
var options = builder.Configuration.GetSection("Scraper").Get<ScraperOptions>() ?? new ScraperOptions();

builder.Services.AddSingleton(TimeProvider.System);


builder.Services.AddKafkaEventPublisher(builder.Configuration);

// HttpClient with the Polly retry/backoff policy applied to every provider request.
builder.Services
    .AddHttpClient("provider", client =>
    {
        client.BaseAddress = new Uri(options.Provider.BaseUrl);
        client.Timeout = TimeSpan.FromSeconds(30);
    })
    .AddPolicyHandler(PollyPolicies.BuildRetryPolicy(options.Provider.Backoff));

builder.Services.AddSingleton<IResponseValidator, ResponseValidator>();

builder.Services.AddSingleton<IProviderPoller, ProviderPoller>();

// for simplicity this is a single worker 
// -> I would set a new hosted service per provider if we had multiple providers to poll in parallel 
// Why? so we can "block" one without affecting the others and ensuring "noisy neighbour" issue won't occur 
// Potentially even use a pod per provider if needed (overcomplexing IMO for this task but MUST be considered for production workload)  
builder.Services.AddHostedService<ScraperWorker>();

var host = builder.Build();
await host.RunAsync();
