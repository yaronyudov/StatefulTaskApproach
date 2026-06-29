namespace SportsPipeline.Classifier.Orleans.Models;

public record MatchDelta(string EventType, object NewValue, bool IsCrucial = false);
