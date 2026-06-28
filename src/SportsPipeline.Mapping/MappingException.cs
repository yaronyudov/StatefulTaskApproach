namespace SportsPipeline.Mapping;

/// <summary>Thrown when a provider payload cannot be mapped to the domain shape.</summary>
public sealed class MappingException(string message) : Exception(message);
