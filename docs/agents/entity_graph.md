# Entity Graph & Domain Model

This document outlines the core domain entities and key architectural interfaces of the `StatefulTaskApproach` project using a Mermaid diagram. It is designed to give AI agents a precise structural understanding of the system's abstractions.

## Domain & Interface Graph

```mermaid
classDiagram
    %% Core Domain Models
    class SportEvent {
        +string SportType
        +string CompetitionType
        +DateTimeOffset StartTime
        +Team HomeTeam
        +Team AwayTeam
        +DateTimeOffset EventTime
        +IReadOnlyDictionary~string, string~ Metadata
        +string MatchKey
    }

    class Team {
        +string Id
        +string Name
    }

    class MatchIdentity {
        <<static>>
        +ComputeKey(sport, competition, homeTeam, awayTeam) string
        +Normalize(value) string
    }

    %% Relationships
    SportEvent "1" *-- "1" Team : HomeTeam
    SportEvent "1" *-- "1" Team : AwayTeam
    SportEvent ..> MatchIdentity : uses for MatchKey

    %% Abstractions (Ports)
    class IMappingStore {
        <<interface>>
        +GetRulesAsync(providerId, cancellationToken)
    }

    class IEventPublisher {
        <<interface>>
        +PublishAsync(matchKey, event, cancellationToken)
    }

    class IMatchSearchStore {
        <<interface>>
        +SearchMatchesAsync(query, cancellationToken)
    }

    class IMatchDetailsStore {
        <<interface>>
        +GetMatchDetailsAsync(matchId, cancellationToken)
    }

    class IDeltaHandler {
        <<interface>>
        +HandleDeltaAsync(delta, cancellationToken)
    }

    class ProviderMapping {
        +string ProviderId
        +object Rules
    }
    
    class QueryParams {
        +DateTimeOffset? From
        +DateTimeOffset? To
        +string? Sport
        +string? Competition
        +string? Team
    }

    IMappingStore ..> ProviderMapping : returns
    IMatchSearchStore ..> QueryParams : uses
```

### Key Concepts for Agents
- **`SportEvent`**: The single normalized DTO used across the entire pipeline after validation. All provider-specific shapes are transformed into this by the Validation Service.
- **`MatchIdentity`**: Crucial for stateful classification. It computes an order-agnostic SHA256 key based on `SportType`, `CompetitionType`, and sorted `Team.Name`s. This is the Kafka partition key for the `validated-events` topic, ensuring all events for a match go to the same Flink node.
- **Ports & Adapters**: The interfaces (`IMappingStore`, `IEventPublisher`, etc.) define the *Ports*. Concrete implementations exist in `SportsPipeline.Infrastructure.*` projects.
