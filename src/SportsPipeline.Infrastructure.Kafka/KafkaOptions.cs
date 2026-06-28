namespace SportsPipeline.Infrastructure.Kafka;

public sealed class KafkaPublisherOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = SportsPipeline.Contracts.Topics.IngestedEvents;
}

public sealed class KafkaConsumerOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = SportsPipeline.Contracts.Topics.MatchDeltas;
}
