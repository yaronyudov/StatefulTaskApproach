# Sports Data Pipeline — Architecture

## 1. Context & Goals

Ingest events from multiple external sports-data providers, normalize each provider's format into one internal domain model, run a **stateful classification**, push live **deltas** to subscribed users, and serve **flexible historical + in-window queries** — all on AWS, without letting query load degrade the ingestion pipeline.

Requirements mapped to this design:

| Requirement | Implementation |
|---|---|
| Provider→domain mapping DB | `SportsPipeline.Validator` + MongoDB |
| Scrapers with validation, rate-limit compliance | `SportsPipeline.Scraper` |
| Push processed data to Message Broker | Confluent.Kafka producer |
| Stateful Classification & Aggregation | `SportsPipeline.Classifier.Orleans` (Microsoft Orleans) |
| Persistent Datastores | OpenSearch (Discovery), MongoDB (Details) |
| Live deltas to subscribers (SSE) | Orleans → Redis Pub/Sub → `SportsPipeline.Sse` |
| High-Concurrency Query Caching | `SportsPipeline.QueryApi` + Redis (Semaphore Stampede Protection) |

## 2. Component & Data Flow

You can choose between two processing engines (Microsoft Orleans or Apache Flink) based on your team's expertise. (See [Orleans vs Flink Trade-offs](orleans_vs_flink_v2.md) for details on why both are supported and their specific trade-offs).

### Option A: Microsoft Orleans (Actor Model)
```mermaid
flowchart LR
  P["Providers"] --> SC["Scrapers"]
  SC --> KR[("Kafka Raw Events")]
  KR --> VAL["Validation & Canonical Mapping"]
  VAL --> KV[("Kafka Validated Events")]
  KV --> ORL["Orleans Cluster MatchGrains"]
  
  ORL -->|State Updates| MG[("MongoDB Details")]
  MG -.->|Change Stream CDC| OS[("OpenSearch Discovery")]
  ORL -.->|Deltas| RED[("Redis Pub Sub & Cache")]
  ORL -.->|Invalidate Cache| RED
  
  RED -.->|Deltas| SSE["SSE Service"]
  SSE -.->|Server-Sent Events| U(("Clients"))
  
  Q["Query API"] -->|Search| OS
  Q -->|Get Details| RED
  RED -->|Cache Miss| MG
```

### Option B: Apache Flink (Stream Processing)
```mermaid
flowchart LR
  P["Providers"] --> SC["Scrapers"]
  SC --> KR[("Kafka Raw Events")]
  KR --> VAL["Validation & Canonical Mapping"]
  VAL --> KV[("Kafka Validated Events")]
  KV --> FL{"Apache Flink Job"}
  
  FL -->|Sink Exactly-Once| MG[("MongoDB Details")]
  FL -->|Sink Exactly-Once| OS[("OpenSearch Discovery")]
  FL -.->|Sink| RED[("Redis Pub Sub & Cache")]
  FL -.->|Invalidate Cache| RED
  
  RED -.->|Deltas| SSE["SSE Service"]
  SSE -.->|Server-Sent Events| U(("Clients"))
  
  Q["Query API"] -->|Search| OS
  Q -->|Get Details| RED
  RED -->|Cache Miss| MG
```

Ingestion (left of Orleans/Flink) and query (Query API) never share a datastore connection — a textbook **CQRS** split. The read stores are populated asynchronously by the classification engines.

## 3. Domain Model & Match Identity

Domain DTO (`SportsPipeline.Domain/SportEvent.cs`): `SportType, CompetitionType, StartTime, HomeTeam, AwayTeam, EventTime, Metadata`. Teams are `{ Id, Name }`.

**Order-agnostic match key** (`SportsPipeline.Domain/MatchIdentity.cs`): normalizes each component (trim, lower-invariant, strip diacritics), **sorts the two team names**, joins `sport|competition|teamA|teamB`, and hashes via `SHA-256`. 
*Arsenal-vs-Chelsea* and *Chelsea-vs-Arsenal* collapse to one key. This key is the **Kafka partition key** and the **Orleans Grain Primary Key**.

## 4. Scraper & Validation

One **shared codebase**, deployed as **one instance per provider**.
*Why per-provider?* Independent rate-limit/backoff state, blast-radius isolation, independent scaling.

Pipeline per poll:
1. **Rate-limit compliance** — client-side token bucket (`System.Threading.RateLimiting`).
2. **Resilient fetch** — Polly retry with **exponential backoff + jitter**.
3. **Validation & Canonical Mapping** — JSON → domain via MongoDB rules (`ProviderEventMapper.cs`). This translates raw provider events to a single "canonical" Domain Model, aligning "our side" of the system with the infinite variations of provider formats.
4. **Deduplication** — Edge hashing limits pushing identical payloads.
5. **Publish** — Idempotent producer to Kafka.

### Canonical Mapping Sequence (Raw to Validated)
To ensure the pipeline is agnostic to infinite provider variations, all raw events pass through a mapping phase before entering the main classification engine.

```mermaid
sequenceDiagram
  participant RawKafka as Kafka (Raw Events)
  participant ValService as Validation Service
  participant Mapper as Canonical Mapper
  participant ValKafka as Kafka (Validated Events)
  
  RawKafka->>ValService: Consume Raw Provider JSON/XML
  ValService->>Mapper: Pass Raw Event
  
  Note over Mapper: Canonical Mapping Phase
  Mapper->>Mapper: Extract Sport, Competition, Teams
  Mapper->>Mapper: Normalize Team Names & Sort Alphabetically
  Mapper->>Mapper: Generate Standard MatchKey
  Mapper->>Mapper: Map Provider Status to Canonical Status
  
  Mapper-->>ValService: Canonical Domain SportEvent
  
  ValService->>ValKafka: Publish Validated SportEvent
  Note over ValKafka: Downstream classifiers now agnostic to Provider origin
```

## 5. Stateful Classification

Depending on the engine chosen, the internal flow differs significantly.

### Option A: Microsoft Orleans (Detailed Grain Flow)

Orleans uses Virtual Actors (Grains). The grain lives in RAM and uses MongoDB as its source of truth, with CDC syncing OpenSearch.

```mermaid
stateDiagram-v2
  [*] --> Unactivated
  Unactivated --> Active: Event arrives for MatchKey
  Active --> Active: Event within 2h window\nPublish Delta to Redis
  Active --> WriteToDB: Delta IsCrucial (Goal/Card)
  WriteToDB --> Active: Invalidate Redis Cache\nFlush to Mongo
  Active --> Active: Event outside window (New Match)
  Active --> [*]: Idle for > 2 hours (Deactivation)
```

1. **Virtual Actors:** We don't manually create Grains. When Kafka pushes an event for `match-123`, Orleans automatically routes the message to the single `MatchGrain` responsible for `match-123`, activating it if it isn't already running.
2. **In-Memory Speed:** The `MatchGrain` holds the current `SportState` in RAM. Evaluating rules and computing diffs/deltas requires **zero network hops** to an external database.
3. **Smart Write-Behind (MongoDB Hot-Path Avoidance):**
   - When a delta is calculated, it's immediately published to Redis.
   - If the delta is highly frequent but low value (e.g., a pass), the Grain stays in RAM. A background timer flushes it to MongoDB every 10 seconds.
   - If the delta `IsCrucial` (e.g., Goal, Red Card), the Grain **immediately** writes to MongoDB to guarantee persistence.

#### Detailed MatchGrain Processing Sequence
This outlines the exact flow of `ProcessEventAsync` from Kafka into MongoDB. 

> **Important Task Context:** As part of the basic home task requirements, if an event was found within the `+/- 2 hours` window (meaning it's a duplicate or related update), we should have **STOPPED** processing and just dropped it to satisfy the basic "dedup" requirement. However, in this implementation, we simply log "dedup for task found" and **CONTINUE** processing the event through the diffing engine as an advanced challenge.

```mermaid
sequenceDiagram
  participant Kafka as Validated Kafka Topic
  participant Grain as MatchGrain
  participant RAM as Grain RAM
  participant Mongo as MongoDB
  
  Kafka->>Grain: ProcessEventAsync(SportEvent)
  Grain->>RAM: Find Window (+/- 2 Hours)
  
  alt Window Not Found
    Grain->>RAM: Create New Window & Set Metadata
    Grain->>Mongo: Write Full State (Creates CDC event)
  else Window Found
    Note over Grain: HOME TASK: We should have dropped the event here for basic dedup.
    Note over Grain: CHALLENGE: We log 'dedup' and continue processing to diff state!
    Grain->>Grain: Log "dedup for task found"
  end
  
  Grain->>RAM: Check High-Water Mark (Idempotency)
  alt Event is Older
    Grain-->>Kafka: Ignore & Return
  end
  
  Grain->>Grain: Diff New JSON vs Current State
  alt Crucial Deltas
    Grain->>Mongo: Write Full State Immediately
  else Non-Crucial Deltas
    Grain->>RAM: Mark as Dirty (Background Flush)
  end
```

### Option B: Apache Flink (Detailed Job Flow)

Flink uses keyed streams and keyed state (`ValueState<Match>`). It guarantees exact-once semantics using Two-Phase Commit to multiple sinks, eliminating the need for CDC for OpenSearch.

```mermaid
flowchart TD
  Start(["Event Stream Arrives"]) --> KeyBy["keyBy MatchKey"]
  
  KeyBy --> StateCheck{"Check Flink KeyedState"}
  
  StateCheck -- "Match Exists" --> Diff["Compute Diff/Deltas"]
  Diff --> UpdateState["Update KeyedState"]
  
  StateCheck -- "New Match" --> CreateState["Create New State"]
  
  UpdateState --> SinkSplit{"Split Stream"}
  CreateState --> SinkSplit
  
  SinkSplit -->|Main Sink| MongoSink[("Sink: MongoDB Details")]
  SinkSplit -->|Discovery Sink| OSSink[("Sink: OpenSearch Discovery")]
  SinkSplit -->|Delta Sink| RedisSink[("Sink: Redis Pub/Sub")]
```

## 6. Live Deltas over SSE (Fan-Out Architecture)

```mermaid
sequenceDiagram
  participant Grain as MatchGrain (Orleans)
  participant Redis as Redis Pub/Sub
  participant SSE as SSE Service (.NET)
  participant Client
  
  Client->>SSE: GET /subscribe/{matchId}
  SSE->>Redis: Subscribe to match-deltas:{matchId}
  Grain->>Redis: Publish delta
  Redis->>SSE: Consume delta message
  SSE-->>Client: data: {delta}\n\n
```

We use a **Redis Backplane** to bridge the Orleans processing layer and the stateless Web API layer.
* `SportsPipeline.Classifier.Orleans` pushes lightweight JSON deltas to a Redis Pub/Sub channel.
* `SportsPipeline.Sse` web nodes subscribe to Redis and fan out the messages to connected HTTP clients using Server-Sent Events (SSE).
* This ensures the web nodes remain stateless and perfectly horizontally scalable.

> **Note:** For a comprehensive breakdown of alternative fan-out architectures (including Kafka+SSE and Native Orleans Streams) and why Redis Pub/Sub was chosen, see the [Fan-Out Architecture & Caching Strategy](architecture-fanout.md) document.

## 7. Query Path & Caching (Thundering Herd Protection)

The `SportsPipeline.QueryApi` handles two primary operations using isolated databases:

- **OpenSearch (Discovery):** Ad-hoc searching using 1-4 parameters (sport, competition, team, time). Returns a list of `MatchSummary` objects.
- **MongoDB (Details):** Point lookup by `matchId`.

### Caching Strategy & Cache Invalidation
To protect MongoDB from catastrophic traffic spikes, we aggressively cache the full match details in Redis.
* **Non-Live Matches:** Safely cached indefinitely (24h TTL) since the game has concluded.
* **Live Matches:** When Orleans processes a crucial update (like a goal), it immediately calls `ICacheInvalidator`, actively deleting the Redis key. The very next user query will fetch the fresh score from MongoDB and re-seed the cache.

### Stampede Prevention (Request Coalescing)
If the cache is empty (or just invalidated) and 10,000 users simultaneously request the match details, a naive implementation would blast MongoDB with 10,000 identical queries.

```mermaid
sequenceDiagram
  participant Client1 as Client 1
  participant ClientN as Clients 2-10k
  participant API as Query API
  participant Redis as Redis Cache
  participant Mongo as MongoDB
  
  Client1->>API: GET /matches/{id}
  ClientN->>API: GET /matches/{id}
  
  API->>Redis: GetStringAsync (Miss)
  
  Note over API: Client 1 kicks off DB fetch Task.<br/>Clients 2-10k await the exact same Task.
  
  API->>Mongo: Fetch Details (ONLY 1 CALL)
  Mongo-->>API: Match Details
  API->>Redis: SetStringAsync (Cache result)
  
  Note over API: Task resolves. All clients get data.
  
  API-->>Client1: JSON Response
  API-->>ClientN: JSON Response (Returned Simultaneously)
```

We implemented `CacheStampedeProtector` using an in-memory `ConcurrentDictionary<string, Task<string>>` (Request Coalescing/Task Multiplexing). Exactly **1** request is allowed to query MongoDB. The other 9,999 requests await the exact same Task and resolve simultaneously without clogging threads.

## 8. Failure Modes

- **Provider outage / 429** → Polly backoff + circuit breaker; isolated to that provider's instance.
- **Poison record** → dropped with a warning in the scraper; never blocks the poll loop.
- **Orleans Node Crash** → Orleans automatically resurrects the `MatchGrain` on another healthy node. It rehydrates its state from MongoDB and resumes processing seamlessly.
- **SSE slow client** → bounded per-subscriber channel drops oldest, never blocks the consumer.
- **Traffic Spike** → Thundering Herd protection guarantees 10,000 API requests result in 1 DB query.

## 9. AWS Mapping (Deployment)

Kafka (Amazon MSK) · OpenSearch Service (Discovery) · MongoDB Atlas on AWS (Details) · ECS Fargate (Scrapers, Query API, SSE) · ElastiCache Redis (Pub/Sub & Cache) · Secrets Manager · ALB.
