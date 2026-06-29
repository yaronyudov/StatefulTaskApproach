namespace SportsPipeline.Infrastructure.OpenSearch;

public sealed class OpenSearchOptions
{
    public string Url { get; set; } = "http://localhost:9200";
    public string Index { get; set; } = "matches";
}
