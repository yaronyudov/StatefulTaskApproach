using SportsPipeline.Classifier.Orleans.Models;

namespace SportsPipeline.Classifier.Orleans.Tests;

public class FootballStateTests
{
    [Fact]
    public void GetChanges_ShouldReturnOnlyYellowCardDelta_WhenOnlyYellowCardChanges()
    {
        // Arrange
        var oldState = new FootballState(1, 0, "LIVE", 0, 0, 0, 0, false, false);
        var newState = new FootballState(1, 0, "LIVE", 0, 0, 1, 0, false, false); // Provider B sent Goal + YC

        // Act
        var deltas = oldState.GetChanges(newState);

        // Assert
        Assert.Single(deltas);
        Assert.Equal("YellowCardsHomeChanged", deltas[0].EventType);
        Assert.Equal(1, deltas[0].NewValue);
    }

    [Fact]
    public void GetChanges_ShouldReturnEmpty_WhenNoChanges()
    {
        // Arrange
        var oldState = new FootballState(1, 0, "LIVE", 0, 0, 1, 0, false, false);
        var newState = new FootballState(1, 0, "LIVE", 0, 0, 1, 0, false, false); // Provider A delayed Goal + YC

        // Act
        var deltas = oldState.GetChanges(newState);

        // Assert
        Assert.Empty(deltas);
    }
}
