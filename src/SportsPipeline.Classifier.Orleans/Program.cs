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

var redisConnStr = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING") ?? "localhost:6379";

builder.UseOrleans(silo =>
{
    var mongoConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING") ?? "mongodb://localhost:27017";

    // Cluster membership stays on MongoDB (low volume, off the hot path)...
    silo.UseMongoDBClient(mongoConnectionString)
        .UseMongoDBClustering(options =>
        {
            options.DatabaseName = "SportsPipeline";
            options.CreateShardKeyForCosmos = false;
        })
        // ...but grain state now lives in Redis: per-event persistence is cheap and survives a pod
        // crash, so MongoDB is no longer written on the hot path (it becomes the periodic archive).
        .AddRedisGrainStorageAsDefault(options =>
        {
            options.ConfigurationOptions = ConfigurationOptions.Parse(redisConnStr);
        });
});

builder.ConfigureServices((hostContext, services) =>
{
    services.Configure<KafkaOptions>(options =>
    {
        options.BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";

        // Throughput / scale-out tunables (all optional; sensible defaults live in KafkaOptions).
        if (int.TryParse(Environment.GetEnvironmentVariable("KAFKA_BATCH_SIZE"), out var batchSize))
            options.BatchSize = batchSize;
        if (int.TryParse(Environment.GetEnvironmentVariable("KAFKA_MAX_CONCURRENCY"), out var maxConcurrency))
            options.MaxConcurrency = maxConcurrency;
        if (int.TryParse(Environment.GetEnvironmentVariable("KAFKA_CONSUMER_COUNT"), out var consumerCount))
            options.ConsumerCount = consumerCount;
        if (int.TryParse(Environment.GetEnvironmentVariable("KAFKA_PARTITIONS"), out var partitions))
            options.Partitions = partitions;
    });

    // Discovery (OpenSearch) is written directly by the grain on first sight of a match.
    services.AddOpenSearchMatchSearch(hostContext.Configuration);
    // MongoDB is the periodic / final archive (written off the hot path by the grain's flush timer).
    services.AddMongoMatchDetailsStore(hostContext.Configuration);

    services.AddSingleton<IValidatedEventProcessor, OrleansEventProcessor>();
    services.AddHostedService<KafkaValidatedEventConsumer>();

    // Redis: the authoritative live store + SSE delta fan-out.
    services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisConnStr));
    services.AddSingleton<IDeltaPublisher, RedisDeltaPublisher>();
    services.AddSingleton<ILiveMatchStateStore, RedisLiveMatchStateStore>();
});

var host = builder.Build();
await host.RunAsync();
