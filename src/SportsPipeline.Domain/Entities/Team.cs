namespace SportsPipeline.Domain;

/// <summary>
/// A team participating in a match. <see cref="Id"/> is the provider/domain identifier;
/// <see cref="Name"/> is the human-readable name used (after normalization) to build the
/// order-agnostic <see cref="MatchIdentity"/>.
/// </summary>
public sealed record Team(string Id, string Name);
