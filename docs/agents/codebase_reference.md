# Codebase Reference

This document provides a technical breakdown of each project within the `StatefulTaskApproach` solution. It details the responsibilities and key files for AI agents navigating the codebase.

## Project Structure & Responsibilities

The solution follows a Ports and Adapters (Hexagonal) Architecture. Dependencies flow inwards towards `SportsPipeline.Domain` and `SportsPipeline.Abstractions`. The application projects only depend on the abstractions, while the infrastructure projects provide concrete implementations.

### 1. Core Projects

#### `SportsPipeline.Domain`
*   **Purpose**: Contains the central business objects and domain logic. This project has no external dependencies.
*   **Key Files**:
    *   `SportEvent.cs`: The unified, internal representation of an incoming match event.
    *   `Team.cs`: Simple team representation (`Id`, `Name`).
    *   `MatchIdentity.cs`: Computes a stable, order-agnostic Kafka partition key (`MatchKey`) from the sport, competition, and team names.

#### `SportsPipeline.Abstractions`
*   **Purpose**: Defines the "Ports" (interfaces) that the core applications interact with.
*   **Key Files**:
    *   `IMappingStore.cs`: Interface for fetching provider mapping rules.
    *   `IEventPublisher.cs`: Interface for publishing mapped events to the ingestion queue (Kafka).
    *   `IMatchSearchStore.cs`: Interface for the discovery/search datastore (OpenSearch).
    *   `IMatchDetailsStore.cs`: Interface for fetching detailed match records (MongoDB).
    *   `IDeltaHandler.cs`: Interface for processing live match updates.

#### `SportsPipeline.Contracts`
*   **Purpose**: Contains definitions for Kafka topic names and shared JSON contracts used for serialization across distributed components.

---

### 2. Infrastructure (Adapters)

These projects implement the interfaces defined in `SportsPipeline.Abstractions`. They encapsulate all external data and I/O.

*   **`SportsPipeline.Infrastructure.Kafka`**: Implements `IEventPublisher` (via `KafkaEventPublisher`) to send events to MSK, and contains consumers for reading deltas.
*   **`SportsPipeline.Infrastructure.Mongo`**: Implements both `IMatchDetailsStore` for match data and `IMappingStore` for provider mapping rules using MongoDB.
*   **`SportsPipeline.Infrastructure.Files`**: A local-development implementation of `IMappingStore` (using JSON files).
*   **`SportsPipeline.Infrastructure.OpenSearch`**: Implements `IMatchSearchStore` to provide fast, multi-parameter discovery queries.

---

### 3. Applications

#### `SportsPipeline.Scraper`
*   **Purpose**: The ingestion entry point. It pulls data from third-party providers. Designed to run as one instance per provider.
*   **Flow**: Polls Provider API → Applies Rate Limiting / Backoff (`Polly`) → Publishes raw provider JSON to Kafka `raw-events` topic.

#### `SportsPipeline.Validator` [NEW MVP Component]
*   **Purpose**: A dedicated service that consumes raw provider data, maps it to the internal domain model using `ProviderEventMapper`, and validates it.
*   **Flow**: Consumes `raw-events` → Maps to `SportEvent` → Validates schema/data → Publishes to `validated-events` topic using `MatchKey` as the partition key.

#### `SportsPipeline.QueryApi`
*   **Purpose**: A CQRS-based REST API for querying matches. It completely decouples query load from the ingestion pipeline.
*   **Flow**: `GET /matches` searches OpenSearch for discovery (`IMatchSearchStore`). `GET /matches/{matchId}` retrieves the full document from MongoDB (`IMatchDetailsStore`).

#### `SportsPipeline.Sse` [V2 Feature - Commented Out]
*   **Purpose**: A server-sent events (SSE) broker that pushes live match deltas to subscribed clients.
*   **Flow**: In V2, consumes from the Kafka `dedup-topic` (using a unique consumer group per instance for implicit fan-out) and streams updates to clients via `text/event-stream`.

---

### 4. Stream Processing (Flink) **[FUTURE / OUT OF SCOPE]**

> **MVP Implementation Note:** The Flink pipeline is out of scope for the current local C# implementation. The core classification logic (`MatchWindowClassifier`) runs via a simulated `MockFlinkSinkHandler` directly inside the `SportsPipeline.Tests` project to achieve End-to-End testability without a running Flink cluster.

The future stream processor sits between the Validation Service (Kafka `validated-events` topic) and the read stores / SSE broker. It performs stateful windowing (±2 hours) to classify events.

*   **`flink/match_pipeline.sql`**: A declarative Flink SQL job. In MVP, it writes the first event of a match to OpenSearch and MongoDB, and drops subsequent events in the 2h window (dedup).
*   **`flink/java/...`**: A native Java `KeyedProcessFunction` implementation.
*   **[V2 Note]**: In V2, deltas within the window will be emitted to a `dedup-topic` and subjected to "upwards-only" validation.
