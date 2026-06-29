using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SportsPipeline.Abstractions;
using SportsPipeline.Contracts;
using SportsPipeline.Domain;

namespace SportsPipeline.Infrastructure.Kafka;

/// <summary>
/// <see cref="IEventPublisher"/> adapter: publishes domain events to Kafka with an idempotent
/// producer (acks=all, enable.idempotence=true) keyed by the match key, so all events for a fixture
/// land on one partition and are consumed in order by the stateful stage.
/// </summary>
public sealed class KafkaEventPublisher : IEventPublisher, IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly KafkaOptions _options;

    public KafkaEventPublisher(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageSendMaxRetries = int.MaxValue,
            CompressionType = CompressionType.Lz4,
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    public async Task PublishAsync(SportEvent ev, CancellationToken cancellationToken)
    {
        var message = new Message<string, byte[]>
        {
            Key = ev.MatchKey,
            Value = SportEventJson.Serialize(ev),
        };

        await _producer.ProduceAsync(_options.PublishTopic, message, cancellationToken);
    }

    public async Task PublishRawAsync(string topic, string rawPayload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        var message = new Message<string, byte[]>
        {
            Value = System.Text.Encoding.UTF8.GetBytes(rawPayload),
        };

        if (headers != null)
        {
            message.Headers = new Headers();
            foreach (var h in headers)
            {
                message.Headers.Add(h.Key, System.Text.Encoding.UTF8.GetBytes(h.Value));
            }
        }

        await _producer.ProduceAsync(topic, message, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
