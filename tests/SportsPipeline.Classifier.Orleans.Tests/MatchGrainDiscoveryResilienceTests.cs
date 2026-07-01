using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;
using SportsPipeline.Abstractions;
using SportsPipeline.Classifier.Orleans.Grains;
using SportsPipeline.Classifier.Orleans.Models;
using SportsPipeline.Contracts;
using SportsPipeline.Domain;
using Moq;
using Xunit;

namespace SportsPipeline.Classifier.Orleans.Tests;

/// <summary>
/// Verifies that a transient OpenSearch outage at first-event does NOT break event processing and that
/// the flush timer re-asserts the discovery row (the resilience CDC used to provide).
/// </summary>
public class MatchGrainDiscoveryResilienceTests : IClassFixture<MatchGrainDiscoveryResilienceTests.ClusterFixture>
{
    /// <summary>Fails the first IndexAsync call, then succeeds; counts attempts/successes.</summary>
    public sealed class FlakyMatchSearchStore : IMatchSearchStore
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        private int _successes;
        public int Successes => Volatile.Read(ref _successes);

        public Task<IReadOnlyList<JsonElement>> SearchAsync(QueryParams query, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<JsonElement>>(System.Array.Empty<JsonElement>());

        public Task IndexAsync(string matchId, string json, CancellationToken ct = default)
        {
            var n = Interlocked.Increment(ref _calls);
            if (n == 1) throw new System.InvalidOperationException("OpenSearch down");
            Interlocked.Increment(ref _successes);
            return Task.CompletedTask;
        }
    }

    // Shared so the test can inspect what the silo's grain actually did.
    public static readonly FlakyMatchSearchStore Search = new();

    public class ClusterFixture : IDisposable
    {
        public TestCluster Cluster { get; }
        public ClusterFixture()
        {
            var builder = new TestClusterBuilder();
            builder.AddSiloBuilderConfigurator<TestSiloConfigurator>();
            Cluster = builder.Build();
            Cluster.Deploy();
        }
        public void Dispose() => Cluster.StopAllSilos();
    }

    public class TestSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddMemoryGrainStorageAsDefault();
            siloBuilder.ConfigureServices(services =>
            {
                // Fast flush timer so the discovery retry fires quickly.
                var config = new ConfigurationBuilder()
                    .AddInMemoryCollection(new Dictionary<string, string?> { ["MatchGrain:FlushIntervalSeconds"] = "1" })
                    .Build();
                services.AddSingleton<IConfiguration>(config);

                services.AddSingleton(new Mock<IDeltaPublisher>().Object);
                services.AddSingleton(new Mock<ILiveMatchStateStore>().Object);
                services.AddSingleton(new Mock<IMatchDetailsStore>().Object);
                services.AddSingleton<IMatchSearchStore>(Search);
            });
        }
    }

    private readonly TestCluster _cluster;
    public MatchGrainDiscoveryResilienceTests(ClusterFixture fixture) => _cluster = fixture.Cluster;

    [Fact]
    public async Task FirstEvent_IndexFailure_IsSwallowed_And_Retried_By_Timer()
    {
        var grain = _cluster.GrainFactory.GetGrain<IMatchGrain>("resilience_test_match");
        var evt = new SportEvent
        {
            SportType = "soccer", CompetitionType = "epl",
            HomeTeam = new Team("1", "Arsenal"), AwayTeam = new Team("2", "Chelsea"),
            StartTime = DateTimeOffset.UtcNow, EventTime = DateTimeOffset.UtcNow,
            Metadata = new Dictionary<string, string>
            {
                ["providerId"] = "A",
                ["rawJson"] = JsonSerializer.Serialize(new FootballState(1, 0, "LIVE", 0, 0, 0, 0, false, false))
            }
        };

        // The first-event discovery index throws inside the grain; this must NOT surface to the caller.
        await grain.ProcessEventAsync(SportEventJson.Serialize(evt));
        Assert.True(Search.Calls >= 1, "discovery should have been attempted on first event");

        // The flush timer should re-assert discovery (idempotent) and eventually succeed.
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (Search.Successes == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(250);
        }

        Assert.True(Search.Successes >= 1, "discovery should have been re-indexed successfully by the flush timer");
        Assert.True(Search.Calls >= 2, "discovery should have been retried after the initial failure");
    }
}
