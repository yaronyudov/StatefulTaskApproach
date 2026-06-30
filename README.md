# StatefulTaskApproach — Sports Data Pipeline

An AWS sports-data pipeline: ingest from many providers, normalize to one domain model, run a
**stateful first-vs-not-first** classification, push live **deltas** over SSE, and serve flexible
historical queries — with ingestion and query load fully decoupled (CQRS).

## Architecture

See **[docs/part-a-system-design.md](docs/part-a-system-design.md)** for the high-level visual overview mapping out the system design and home task requirements.

See **[docs/architecture.md](docs/architecture.md)** for the full design (diagrams, state machine,
CQRS read path, failure modes, AWS mapping).

```
Providers → Scraper(s) → Kafka(raw-events) → Validator → Kafka(validated-events) →
  classifier (Flink OR Orleans, first vs not-first) → {
    first-match-events, not-first-match-events,
    match-deltas → SSE → subscribers,
    OpenSearch(first = discovery),
    live state → Redis (authoritative while in-progress) → MongoDB (archive at/after match end)
}
Query API: search OpenSearch (find matchId) → fetch details (Redis if live, else MongoDB)
```

## Layout

| Path | What |
|---|---|
| `src/SportsPipeline.Domain` | Domain DTOs + order-agnostic `MatchIdentity` (no deps) |
| `src/SportsPipeline.Abstractions` | **Ports**: `IMappingStore`, `IEventPublisher`, `IMatchSearchStore`, `IMatchDetailsStore`, `IDeltaHandler` + their DTOs |
| `src/SportsPipeline.Contracts` | Kafka topic names + JSON contract |
| `src/SportsPipeline.Validator` | Consumes raw events, maps them (`ProviderEventMapper`), and validates |
| `src/SportsPipeline.Infrastructure.Kafka` | Adapter: `KafkaEventPublisher`, `KafkaDeltaConsumer` |
| `src/SportsPipeline.Infrastructure.DynamoDb` | Adapter: `DynamoDbMappingStore` |
| `src/SportsPipeline.Infrastructure.Files` | Adapter: `JsonFileMappingStore` (local dev) |
| `src/SportsPipeline.Infrastructure.OpenSearch` | Adapter: `OpenSearchMatchSearch` (discovery) |
| `src/SportsPipeline.Infrastructure.Mongo` | Adapter: `MongoMatchDetails` (details) |
| `src/SportsPipeline.Scraper` | App: config-driven poller (rate-limit, backoff, validate, publish) |
| `src/SportsPipeline.Sse` | App: SSE service, `match-deltas` → `text/event-stream` |
| `src/SportsPipeline.QueryApi` | App: discovery (OpenSearch) + details (MongoDB) |
| `flink/match_pipeline.sql` | Shipped Flink SQL classification (relaxed semantics) |
| `flink/java/...` | [FUTURE] Native Java `KeyedProcessFunction` (exact ±2h semantics) |
| `tests/SportsPipeline.Tests` | Unit tests and `MockFlinkSinkHandler` for local E2E simulation |
| `deploy/` | docker-compose stack, Dockerfiles, mock provider, CDK skeleton |

### Layering (Ports & Adapters)

Dependencies point inward: `Domain` → `Abstractions` (ports) → apps. The **apps depend only on the
port interfaces**; each concrete data/IO adapter lives in its own `Infrastructure.<tech>` project and
is selected once in the app's `Program.cs` (composition root). So a service can swap OpenSearch, Mongo,
Kafka, or DynamoDB without touching app logic, and each app pulls only the adapters it actually uses
(the scraper never references the Mongo/OpenSearch drivers).

## Build & test

```bash
dotnet test StatefulTaskApproach.sln -c Release
```

## Run locally

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Then follow **[deploy/README.md](deploy/README.md)** to submit the Flink job and exercise the
discovery search, details lookup, and live SSE stream.
