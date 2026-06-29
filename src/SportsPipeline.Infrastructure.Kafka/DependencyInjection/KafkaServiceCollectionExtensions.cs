using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Kafka;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaEventPublisher(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection("Kafka"));
        services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
        return services;
    }

    public static IServiceCollection AddKafkaRawEventConsumer(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection("Kafka"));
        services.AddHostedService<KafkaRawEventConsumer>();
        return services;
    }

    public static IServiceCollection AddKafkaDeltaConsumer(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaOptions>(config.GetSection("Kafka"));
        services.AddHostedService<KafkaDeltaConsumer>();
        return services;
    }
}
