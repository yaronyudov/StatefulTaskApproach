using Microsoft.Extensions.Logging.Abstractions;
using SportsPipeline.Sse;
using System.Threading.Channels;
using Xunit;

namespace SportsPipeline.Tests.E2E;

public class SseE2ETests
{
    [Fact]
    public async Task SsePipeline_Should_FanOut_Delta_To_Subscribers()
    {
        // 1. Arrange
        var broker = new DeltaBroker();
        var handler = new BrokerDeltaHandler(broker);
        var matchId = "test-match-123";
        var payload = "{\"EventTime\":\"2026-06-28T18:00:00Z\",\"EventType\":\"Goal\",\"NewValue\":\"1\"}";

        // 2. Act - Client Subscribes
        var (subId, reader) = broker.Subscribe(matchId);

        // Simulate Kafka/Redis pushing a message
        handler.Handle(matchId, payload);

        // 3. Assert
        var receivedPayload = await reader.ReadAsync();
        
        Assert.Equal(payload, receivedPayload);
        
        // Ensure graceful unsubscribe
        broker.Unsubscribe(matchId, subId);
    }
}
