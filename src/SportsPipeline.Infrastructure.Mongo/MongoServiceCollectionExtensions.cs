using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.Mongo;

public static class MongoServiceCollectionExtensions
{
    public static IServiceCollection AddMongoMatchDetailsStore(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<MongoOptions>(config.GetSection("Mongo"));
        
        services.AddSingleton<IMatchDetailsStore>(sp => 
        {
            var options = sp.GetRequiredService<IOptions<MongoOptions>>().Value;
            var mongo = new MongoClient(options.ConnectionString)
                .GetDatabase(options.Database)
                .GetCollection<BsonDocument>(options.Collection);
            return new MongoMatchDetailsStore(mongo);
        });

        return services;
    }

}
