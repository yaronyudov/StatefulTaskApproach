using SportsPipeline.Domain;
using Xunit;

namespace SportsPipeline.Tests;

public class MatchIdentityTests
{
    [Fact]
    public void Key_is_order_agnostic_for_home_and_away()
    {
        var a = MatchIdentity.ComputeKey("Football", "EPL", "Arsenal", "Chelsea");
        var b = MatchIdentity.ComputeKey("Football", "EPL", "Chelsea", "Arsenal");
        Assert.Equal(a, b);
    }

    [Theory]
    [InlineData("Arsenal", "ARSENAL")]
    [InlineData("Arsenal", "  arsenal  ")]
    [InlineData("Bayern München", "Bayern Munchen")]
    [InlineData("Real  Madrid", "real madrid")]
    public void Key_normalizes_case_whitespace_and_diacritics(string variant, string canonical)
    {
        var a = MatchIdentity.ComputeKey("Football", "EPL", variant, "Chelsea");
        var b = MatchIdentity.ComputeKey("Football", "EPL", canonical, "Chelsea");
        Assert.Equal(a, b);
    }

    [Fact]
    public void Key_differs_for_different_competition_or_sport()
    {
        var epl = MatchIdentity.ComputeKey("Football", "EPL", "Arsenal", "Chelsea");
        var ucl = MatchIdentity.ComputeKey("Football", "UCL", "Arsenal", "Chelsea");
        var basket = MatchIdentity.ComputeKey("Basketball", "EPL", "Arsenal", "Chelsea");

        Assert.NotEqual(epl, ucl);
        Assert.NotEqual(epl, basket);
    }

    [Fact]
    public void Key_differs_for_different_opponents()
    {
        var ac = MatchIdentity.ComputeKey("Football", "EPL", "Arsenal", "Chelsea");
        var al = MatchIdentity.ComputeKey("Football", "EPL", "Arsenal", "Liverpool");
        Assert.NotEqual(ac, al);
    }
}
