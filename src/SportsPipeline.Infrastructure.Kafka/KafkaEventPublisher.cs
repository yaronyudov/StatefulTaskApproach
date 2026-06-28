using Confluent.Kafka;
using SportsPipeline.Abstractions;
using SportsPipeline.Contracts;
using SportsPipeline.Domain;

namespace SportsPipeline.Infrastructure.Kafka;

/// <summary>
/// <see cref="IEventPublisher"/> adapter: publishes domain events to Kafka with an idempotent
/// producer (acks=all, enable.idempotence=true) keyed by the match key, so all events for a fixture
/// land on one partition and are consumed in order by the stateful stage.
/// </summary>
public sealed class KafkaEventPublisher : IEventPublisher
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;

    public KafkaEventPublisher(KafkaPublisherOptions options)
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
