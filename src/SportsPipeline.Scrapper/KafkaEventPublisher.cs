using Confluent.Kafka;
using SportsPipeline.Contracts;
using SportsPipeline.Domain;

namespace SportsPipeline.Scrapper;

public interface IEventPublisher : IAsyncDisposable
{
    Task PublishAsync(SportEvent ev, CancellationToken cancellationToken);
}

/// <summary>
/// Publishes domain events to Kafka with an idempotent producer (acks=all, enable.idempotence=true)
/// keyed by the match key. Keying by match key guarantees all events for a fixture land on the same
/// partition and are therefore consumed in order by the stateful stream processor.
/// </summary>
public sealed class KafkaEventPublisher : IEventPublisher
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;

    public KafkaEventPublisher(KafkaOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageSendMaxRetries = int.MaxValue,
            CompressionType = CompressionType.Lz4,
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
        _topic = options.Topic;
    }

    public async Task PublishAsync(SportEvent ev, CancellationToken cancellationToken)
    {
        var message = new Message<string, byte[]>
        {
            Key = ev.MatchKey,
            Value = SportEventJson.Serialize(ev),
        };

        await _producer.ProduceAsync(_topic, message, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
