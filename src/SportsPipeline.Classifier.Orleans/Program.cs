using SportsPipeline.Abstractions;
using SportsPipeline.Classifier.Orleans.Workers;
using SportsPipeline.Infrastructure.Kafka;
using SportsPipeline.Infrastructure.Kafka.Consumers;
using SportsPipeline.Infrastructure.OpenSearch;
using SportsPipeline.Infrastructure.Redis;
using SportsPipeline.Infrastructure.Mongo;
using Orleans.Configuration;
using StackExchange.Redis;

var builder = Host.CreateDefaultBuilder(args);

builder.UseOrleans(silo =>
{
    var mongoConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING") ?? "mongodb://localhost:27017";
    
    silo.UseMongoDBClient(mongoConnectionString)
        .UseMongoDBClustering(options =>
        {
            options.DatabaseName = "SportsPipeline";
            options.CreateShardKeyForCosmos = false;
        })
        .AddMongoDBGrainStorageAsDefault(options =>
        {
            options.DatabaseName = "SportsPipeline";
        });
});

builder.ConfigureServices((hostContext, services) =>
{
    services.Configure<KafkaOptions>(options =>
    {
        options.BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
    });

    services.AddOpenSearchMatchSearch(hostContext.Configuration);

    services.AddSingleton<IValidatedEventProcessor, OrleansEventProcessor>();
    services.AddHostedService<KafkaValidatedEventConsumer>();

    var redisConnStr = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING") ?? "localhost:6379";
    services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisConnStr));
    services.AddSingleton<IDeltaPublisher, RedisDeltaPublisher>();
    services.AddSingleton<ICacheInvalidator, RedisCacheInvalidator>();
    
    // Add CDC worker to tail MongoDB and push to OpenSearch
    services.AddMongoCdcWorker();
});

var host = builder.Build();
await host.RunAsync();
