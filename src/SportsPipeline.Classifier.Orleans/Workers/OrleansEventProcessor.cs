using SportsPipeline.Abstractions;
using SportsPipeline.Classifier.Orleans.Grains;
using SportsPipeline.Contracts;

namespace SportsPipeline.Classifier.Orleans.Workers;

/// <summary>
/// Orleans-specific implementation of the event processor.
/// Bridges the generic byte array payload into the Orleans Grain ecosystem.
/// </summary>
public class OrleansEventProcessor : IValidatedEventProcessor
{
    private readonly IGrainFactory _grainFactory;

    public OrleansEventProcessor(IGrainFactory grainFactory)
    {
        _grainFactory = grainFactory;
    }

    public async Task ProcessEventAsync(byte[] rawEventPayload)
    {
        // 1. Partial deserialization to extract routing info (MatchKey)
        var sportEvent = SportEventJson.Deserialize(rawEventPayload);
        if (sportEvent == null) return;

        // 2. Resolve the MatchGrain
        var grain = _grainFactory.GetGrain<IMatchGrain>(sportEvent.MatchKey);
        
        // 3. Orleans RPC call (forwards the byte array to the Grain for processing and state diffing)
        await grain.ProcessEventAsync(rawEventPayload);
    }
}
