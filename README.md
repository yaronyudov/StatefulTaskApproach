# StatefulTaskApproach — Sports Data Pipeline

An AWS sports-data pipeline: ingest from many providers, normalize to one domain model, run a
**stateful first-vs-not-first** classification, push live **deltas** over SSE, and serve flexible
historical queries — with ingestion and query load fully decoupled (CQRS).

## Architecture

See **[docs/architecture.md](docs/architecture.md)** for the full design (diagrams, state machine,
CQRS read path, failure modes, AWS mapping).

```
Providers → Scrapper(s) → Kafka(ingested-events) → Flink(first vs not-first) → {
    first-match-events, not-first-match-events,
    match-deltas → SSE → subscribers,
    OpenSearch(first = discovery), MongoDB(deltas = details)
}
Query API: search OpenSearch (find matchId) → fetch MongoDB (details)
```

## Layout

| Path | What |
|---|---|
| `src/SportsPipeline.Domain` | Domain DTOs + order-agnostic `MatchIdentity` |
| `src/SportsPipeline.Contracts` | Kafka topic names + JSON contract |
| `src/SportsPipeline.Mapping` | Provider→domain mapping (DynamoDB + local JSON) |
| `src/SportsPipeline.Scrapper` | Config-driven poller: rate-limit, backoff, validate, publish |
| `src/SportsPipeline.Sse` | SSE service: `match-deltas` → `text/event-stream` |
| `src/SportsPipeline.QueryApi` | Discovery (OpenSearch) + details (MongoDB) API |
| `flink/match_pipeline.sql` | Shipped Flink SQL classification (relaxed semantics) |
| `flink/java/...` | Native Java `KeyedProcessFunction` (exact ±2h semantics) |
| `tests/SportsPipeline.Tests` | Unit tests for the pure logic |
| `deploy/` | docker-compose stack, Dockerfiles, mock provider, CDK skeleton |

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
