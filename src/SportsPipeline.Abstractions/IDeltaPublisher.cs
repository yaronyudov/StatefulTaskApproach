using System.Threading;
using System.Threading.Tasks;

namespace SportsPipeline.Abstractions;

public interface IDeltaPublisher
{
    Task PublishDeltaAsync(string matchId, object delta, CancellationToken cancellationToken);
}
