using Moq;
using SportsPipeline.Abstractions;
using SportsPipeline.Infrastructure.Redis;
using Xunit;

namespace SportsPipeline.Tests;

public class RedisFirstMatchDetailsStoreTests
{
    [Fact]
    public async Task GetById_Returns_Live_From_Redis_Without_Hitting_Mongo()
    {
        var live = new Mock<ILiveMatchStateStore>();
        live.Setup(s => s.ReadAsync("m1", It.IsAny<CancellationToken>())).ReturnsAsync("{\"live\":true}");

        var archive = new Mock<IMatchDetailsStore>();

        var sut = new RedisFirstMongoMatchDetailsStore(live.Object, archive.Object);

        var result = await sut.GetByIdAsync("m1");

        Assert.Equal("{\"live\":true}", result);
        archive.Verify(s => s.GetByIdAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task GetById_Falls_Back_To_Mongo_When_Not_Live()
    {
        var live = new Mock<ILiveMatchStateStore>();
        live.Setup(s => s.ReadAsync("m2", It.IsAny<CancellationToken>())).ReturnsAsync((string?)null);

        var archive = new Mock<IMatchDetailsStore>();
        archive.Setup(s => s.GetByIdAsync("m2", It.IsAny<CancellationToken>())).ReturnsAsync("{\"archived\":true}");

        var sut = new RedisFirstMongoMatchDetailsStore(live.Object, archive.Object);

        var result = await sut.GetByIdAsync("m2");

        Assert.Equal("{\"archived\":true}", result);
        archive.Verify(s => s.GetByIdAsync("m2", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Upsert_Writes_Through_To_Mongo_Archive()
    {
        var live = new Mock<ILiveMatchStateStore>();
        var archive = new Mock<IMatchDetailsStore>();

        var sut = new RedisFirstMongoMatchDetailsStore(live.Object, archive.Object);

        await sut.UpsertAsync("m3", "{}");

        archive.Verify(s => s.UpsertAsync("m3", "{}", It.IsAny<CancellationToken>()), Times.Once);
    }
}
