namespace SportsPipeline.Scrapper;

/// <summary>Root configuration for one scrapper instance. One instance serves exactly one provider.</summary>
public sealed class ScrapperOptions
{
    public ProviderConfig Provider { get; set; } = new();
    public KafkaOptions Kafka { get; set; } = new();

    /// <summary>Path to the local mapping JSON file (development). Ignored when DynamoDB is used.</summary>
    public string MappingFilePath { get; set; } = "providers.sample.json";
}

public sealed class ProviderConfig
{
    public string Id { get; set; } = "demo-provider";
    public string BaseUrl { get; set; } = "http://localhost:9099";

    /// <summary>Relative endpoint returning a JSON array of provider events.</summary>
    public string EventsPath { get; set; } = "/events";

    public int PollIntervalSeconds { get; set; } = 10;

    /// <summary>Reference (name) of the credential in AWS Secrets Manager — never the secret value.</summary>
    public string? AuthSecretName { get; set; }

    public RateLimitOptions RateLimit { get; set; } = new();
    public BackoffOptions Backoff { get; set; } = new();
    public ValidationOptions Validation { get; set; } = new();
}

/// <summary>Client-side token bucket so we never exceed the provider's published rate limit.</summary>
public sealed class RateLimitOptions
{
    public int RequestsPerSecond { get; set; } = 5;
    public int Burst { get; set; } = 10;
}

/// <summary>Exponential backoff with jitter for transient failures / HTTP 429.</summary>
public sealed class BackoffOptions
{
    public int BaseDelayMs { get; set; } = 200;
    public int MaxDelayMs { get; set; } = 30_000;
    public int MaxRetries { get; set; } = 5;
    public double JitterFactor { get; set; } = 0.5;
}

/// <summary>Security limits applied to every provider response before it is trusted.</summary>
public sealed class ValidationOptions
{
    public long MaxResponseBytes { get; set; } = 5 * 1024 * 1024;
    public string[] AllowedContentTypes { get; set; } = ["application/json"];
    public int MaxEventsPerResponse { get; set; } = 10_000;
    public int MaxStringLength { get; set; } = 512;

    /// <summary>Reject events whose timestamps fall outside +/- this many days from now (sanity bound).</summary>
    public int MaxTimestampSkewDays { get; set; } = 365;
}

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = SportsPipeline.Contracts.Topics.IngestedEvents;
}
