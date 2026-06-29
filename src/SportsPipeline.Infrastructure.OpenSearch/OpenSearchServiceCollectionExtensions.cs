using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenSearch.Client;
using SportsPipeline.Abstractions;

namespace SportsPipeline.Infrastructure.OpenSearch;

public static class OpenSearchServiceCollectionExtensions
{
    public static IServiceCollection AddOpenSearchMatchSearch(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<OpenSearchOptions>(config.GetSection("OpenSearch"));
        
        services.AddSingleton<IMatchSearchStore>(sp => 
        {
            var options = sp.GetRequiredService<IOptions<OpenSearchOptions>>().Value;
            var osSettings = new ConnectionSettings(new Uri(options.Url)).DefaultIndex(options.Index);
            var osClient = new OpenSearchClient(osSettings);
            return new OpenSearchMatchSearchStore(osClient, options.Index);
        });

        return services;
    }
}
