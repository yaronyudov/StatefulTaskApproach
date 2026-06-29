using SportsPipeline.Domain;

namespace SportsPipeline.Classifier.Orleans.Grains;

public interface IMatchGrain : IGrainWithStringKey
{
    Task ProcessEventAsync(byte[] eventPayload);
}
