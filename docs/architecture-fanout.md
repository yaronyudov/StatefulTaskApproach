# Fan-Out Architecture & Caching Strategy

This document details the trade-offs of the different fan-out architectures evaluated for pushing live match updates (deltas) to clients, as well as the caching strategies implemented across the pipeline.

## Fan-Out Options

### 1. Kafka + SSE (Current Flink Pipeline)
* **How it works:** Flink computes deltas and pushes them to a `match-deltas` Kafka topic. The SSE web nodes consume the topic and fan out to connected WebSockets/SSE clients.
* **Pros:** Complete decoupling. The SSE nodes are stateless and isolated from the processing engine. Any language/tech stack can consume Kafka.
* **Cons:** Unnecessary persistence. Live sports deltas are highly ephemeral (nobody cares about a goal update 2 hours after the match ends). Storing these in Kafka introduces a slight (but noticeable) network hop latency and storage overhead.

### 2. Native Orleans with SignalR (Orleans Only)
* **How it works:** Orleans Grains push updates directly to Orleans Streams. The Web API instances install the Orleans Client SDK, join the cluster, and subscribe directly to the streams.
* **Pros:** Absolute lowest latency. Zero external message brokers.
* **Cons:** Extreme tight coupling. The Web API *must* be written in C# and be aware of Orleans infrastructure. 
* **Verdict:** *If Redis is not used elsewhere in the stack, Native Orleans is preferred for sheer speed. The tight coupling is generally an acceptable trade-off since the organization is already committed to the tech stack.*

### 3. Redis Backplane & SignalR (Implemented Orleans Pipeline)
* **How it works:** Orleans Grains compute deltas and push them to a Redis Pub/Sub channel (`match-deltas:*`). The SSE web nodes subscribe to Redis and fan out to clients.
* **Pros:** The industry-standard approach for scaling ASP.NET SignalR. Ephemeral, low-latency, and decouples the web tier from Orleans internals.
* **Cons:** Requires running Redis.

---

## Caching Strategy

To ensure our databases (OpenSearch and MongoDB) survive massive traffic spikes ("Thundering Herds"), we employ Redis as a highly available cache.

### Non-Live Matches
* **Strategy:** Cached indefinitely in Redis (and additionally cached at the Edge via CDN). Once a match concludes, its data rarely changes, making it safe for aggressive caching.

### Live Matches & Invalidation
* **Strategy:** We heavily cache the full snapshot of live matches in Redis to protect MongoDB.
* **Invalidation:** Caching live statistics presents a challenge:
  * Highly frequent stats (e.g., passes) should *not* invalidate the cache, as it would cause continuous DB hits.
  * Crucial stats (e.g., Goals, Red Cards) *must* be reflected immediately.
* **Implementation:** The Orleans `MatchGrain` determines if an update is `IsCrucial`. If true, Orleans immediately flushes to MongoDB and explicitly triggers `KeyDelete` on the Redis cache. The next read will safely hit the DB and repopulate the cache.

### Thundering Herd Protection
* **Problem:** If a popular match starts and the cache is empty, 10,000 concurrent users hitting the API could trigger 10,000 parallel queries to MongoDB.
* **Solution:** We implemented a `CacheStampedeProtector` (`SemaphoreSlim` in-memory lock per key). When 10,000 requests hit an empty cache, exactly **1** request is allowed to fetch from the DB. The other 9,999 requests wait asynchronously. Once the single DB call completes and populates Redis, the waiting requests read directly from the cache, preventing database collapse.
