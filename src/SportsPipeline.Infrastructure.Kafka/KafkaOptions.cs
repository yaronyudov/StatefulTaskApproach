namespace SportsPipeline.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string PublishTopic { get; set; } = Topics.ValidatedEvents;
    public string ConsumeTopic { get; set; } = Topics.RawEvents;
    public string DeltaTopic { get; set; } = Topics.MatchDeltas;
}
