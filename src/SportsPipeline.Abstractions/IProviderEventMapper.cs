using System.Text.Json;
using SportsPipeline.Domain;

namespace SportsPipeline.Abstractions;

public interface IProviderEventMapper
{
    SportEvent Map(JsonElement source, ProviderMapping mapping);
}
