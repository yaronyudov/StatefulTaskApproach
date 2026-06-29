using System.Threading;
using System.Threading.Tasks;

namespace SportsPipeline.Abstractions;

public interface ICacheInvalidator
{
    Task InvalidateMatchDetailsAsync(string matchId, CancellationToken cancellationToken);
}
