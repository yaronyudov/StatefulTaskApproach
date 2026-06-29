using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Files;
using SportsPipeline.Infrastructure.Kafka;
using SportsPipeline.Validator.Mapping;
using SportsPipeline.Validator;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaEventPublisher(builder.Configuration);
builder.Services.AddKafkaRawEventConsumer(builder.Configuration);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IDomainEventValidator, DomainEventValidator>();
builder.Services.AddSingleton<IProviderEventMapper, ProviderEventMapper>();

// Using local file mappings for MVP to simplify setup
var mappingFilePath = builder.Configuration.GetValue<string>("MappingFilePath") ?? "providers.sample.json";
var mappingStore = await JsonFileMappingStore.LoadAsync(mappingFilePath);
builder.Services.AddSingleton<IMappingStore>(mappingStore);

builder.Services.AddSingleton<IRawEventHandler, RawEventValidatorHandler>();

var host = builder.Build();
await host.RunAsync();
