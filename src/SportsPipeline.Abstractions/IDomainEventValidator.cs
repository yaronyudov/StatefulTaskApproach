using SportsPipeline.Domain;

namespace SportsPipeline.Abstractions;

public interface IDomainEventValidator
{
    SportEvent ValidateEvent(SportEvent ev);
}
