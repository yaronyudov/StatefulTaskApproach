using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SportsPipeline.Abstractions;
using SportsPipeline.Domain;
using System.Text.Json;
using Xunit;

namespace SportsPipeline.Tests.E2E;

public class FullPipelineIntegrationTests
{
    private static DateTimeOffset At(int hour, int minute = 0) =>
        new(2026, 6, 28, hour, minute, 0, TimeSpan.Zero);

    [Fact]
    public async Task Indexer_Simulates_Flink_Deduplication_Full_Flow()
    {
        // 1. Arrange: Mocks for the Sink logic (MongoDB and OpenSearch)
        var mockDetailsStore = new Mock<IMatchDetailsStore>();
        var mockSearchStore = new Mock<IMatchSearchStore>();
        
        var indexerHandler = new MockFlinkSinkHandler(
            mockDetailsStore.Object,
            mockSearchStore.Object,
            NullLogger<MockFlinkSinkHandler>.Instance);

        // Helper to quickly generate validated events
        SportEvent CreateEvent(DateTimeOffset time)
        {
            return new SportEvent
            {
                SportType = "Football",
                CompetitionType = "EPL",
                HomeTeam = new Team("arsenal-id", "Arsenal"),
                AwayTeam = new Team("chelsea-id", "Chelsea"),
                StartTime = time,
                EventTime = time
            };
        }

        // 2. Act: Push the exact sequence of events the user provided
        // Start 18:00
        await indexerHandler.HandleAsync(CreateEvent(At(18, 0)), CancellationToken.None);
        
        // Exact Lower Bound 16:00
        await indexerHandler.HandleAsync(CreateEvent(At(16, 0)), CancellationToken.None);
        
        // Exact Upper Bound 20:00
        await indexerHandler.HandleAsync(CreateEvent(At(20, 0)), CancellationToken.None);
        
        // Just Outside 17:30
        await indexerHandler.HandleAsync(CreateEvent(At(17, 30)), CancellationToken.None);
        
        // Just Inside 18:30
        await indexerHandler.HandleAsync(CreateEvent(At(18, 30)), CancellationToken.None);
        
        // New Game 20:01
        await indexerHandler.HandleAsync(CreateEvent(At(20, 1)), CancellationToken.None);

        // 3. Assert: The stores should only have been written to TWICE (Start and New Game)
        mockDetailsStore.Verify(s => s.UpsertAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        mockSearchStore.Verify(s => s.IndexAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }
}
