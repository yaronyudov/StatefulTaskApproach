using SportsPipeline.Domain;
using SportsPipeline.Scrapper;
using Xunit;

namespace SportsPipeline.Tests;

public class ResponseValidatorTests
{
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static readonly DateTimeOffset Now = new(2026, 6, 27, 12, 0, 0, TimeSpan.Zero);

    private static ResponseValidator Validator(ValidationOptions? options = null) =>
        new(options ?? new ValidationOptions(), new FixedTimeProvider(Now));

    private static SportEvent Event(DateTimeOffset eventTime, string homeName = "Arsenal") => new()
    {
        SportType = "Football",
        CompetitionType = "EPL",
        StartTime = eventTime,
        EventTime = eventTime,
        HomeTeam = new Team("T1", homeName),
        AwayTeam = new Team("T2", "Chelsea"),
    };

    [Fact]
    public void Accepts_event_within_skew_and_trims()
    {
        var validated = Validator().ValidateEvent(Event(Now, homeName: "  Arsenal  "));
        Assert.Equal("Arsenal", validated.HomeTeam.Name);
    }

    [Fact]
    public void Rejects_event_outside_timestamp_skew()
    {
        var options = new ValidationOptions { MaxTimestampSkewDays = 1 };
        Assert.Throws<ValidationException>(() => Validator(options).ValidateEvent(Event(Now.AddDays(5))));
    }

    [Fact]
    public void Rejects_overlong_strings()
    {
        var options = new ValidationOptions { MaxStringLength = 4 };
        Assert.Throws<ValidationException>(() => Validator(options).ValidateEvent(Event(Now, homeName: "TooLongName")));
    }

    [Fact]
    public void Strips_control_characters()
    {
        var withTab = "Arsen" + (char)9 + "al";
        var validated = Validator().ValidateEvent(Event(Now, homeName: withTab));
        Assert.Equal("Arsenal", validated.HomeTeam.Name);
    }

    [Fact]
    public void Rejects_event_count_over_limit()
    {
        var options = new ValidationOptions { MaxEventsPerResponse = 2 };
        Assert.Throws<ValidationException>(() => Validator(options).ValidatePayloadSize(3));
    }
}
