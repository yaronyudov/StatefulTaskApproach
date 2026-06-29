# Architecture Decision Record: Flink vs Orleans (V2 Classifier)

## Context
The system requires a strict stateful evaluation of incoming match events from multiple external data providers. We must determine if an event belongs to a brand new match (creating a new `+/- 2h` window based on kickoff time) or an existing active match. We must also drop older out-of-order data, diff the incoming data to extract exact Deltas (e.g., "Goal Scored"), instantly notify subscribers of those Deltas, and flush the state safely to MongoDB.

Our initial V1 implementation successfully utilized **Apache Flink (Java)**. This document explores the trade-offs of the **V2 Microsoft Orleans (C#)** implementation that replaces Flink.

---

## ⚠️ Mutual Exclusivity Warning
**You must run EITHER Flink OR Orleans, never both simultaneously.** 
Because both engines act as the primary classifier consuming the `validated-events` Kafka topic and writing directly to the `matches` MongoDB collection, running them together will result in duplicated processing, competing writes, and data corruption. 

To easily switch between them, we have configured **Docker Compose Profiles**:
* **Run with Orleans:** `docker compose -f deploy/docker-compose.yml --profile orleans up --build`
* **Run with Flink:** `docker compose -f deploy/docker-compose.yml --profile flink up --build`

---

## The Apache Flink (Java) Approach
*V1 Implementation*

In Flink, the logic is encapsulated inside a `KeyedProcessFunction`. State is maintained in RocksDB on the local node. 

### Pros
1. **Mathematical Exactly-Once Guarantees:** Flink perfectly ties Kafka consumer offsets to its RocksDB state checkpoints. Complete crash immunity.
2. **Built-in Event-Time and Watermarks:** Natively built to handle out-of-order event streams based on their embedded timestamps.
3. **Window Timers:** Natively supports firing an event when exactly 2 hours of stream-time have passed to safely flush aggregated data to MongoDB.

### Cons
1. **Language Barrier:** Requires the team to maintain a separate Java codebase alongside the primary .NET ecosystem.
2. **Strict DAG Structure:** Backpressure flows backwards. If MongoDB has an outage, the Flink MongoDB Sink halts, which completely stalls the entire pipeline (including the real-time fan-out). 
   * **Mitigation:** This stalling issue can easily be fixed by decoupling the DB write. Flink could simply publish the final state to a Kafka topic, and a downstream C# consumer could write it to MongoDB, completely unblocking Flink.
   * **Design Context:** It should be noted that Flink was not designed to be the "all the way" real-time engine for this architecture; its primary purpose was the 2-hour windowing task, with the real-time fan-out acting as an added extra.
3. **Complex Local State:** Replicating business diffing rules (e.g., extracting a "Yellow Card" Delta) inside Java `ValueState` objects requires duplicating DTO structures and logic from the core C# Domain.

---

## The Microsoft Orleans (C#) Approach
*V2 Implementation*

Orleans introduces the "Virtual Actor Model." Every unique `MatchKey` is automatically instantiated as an in-memory `MatchGrain`. A standard C# Kafka Worker consumes `validated-events` and passes them to the correct Grain via RPC.

### Pros
1. **100% C# Ecosystem:** Eliminates the Java requirement. The Grain can reuse the exact same `SportsPipeline.Domain` models, interfaces, and diffing logic as the rest of the stack.
2. **Granular Diffing Engine:** Because the Grain is pure C#, we can easily implement strict `System.Text.Json` partial deserialization to instantly pluck `HomeScore` or `YellowCards` out of a massive JSON payload, calculate discrete `MatchDelta` objects, and ignore the rest. 
3. **Asynchronous Write-Behind Caching:** Flink was designed to hold the state for 2 hours and do one massive write. Orleans solves the hot-path problem via throttled Write-Behind (`WriteStateAsync`). The Grain can update its RAM instantly, fan out the delta instantly, but only flush the full JSON snapshot to MongoDB periodically (or on key state changes), preventing the DB from being hammered.
4. **Resilient Windowing:** To handle overlapping fixtures for the exact same teams (e.g., Today's game interwoven with Next Week's game), the Orleans Grain holds a `List<ActiveMatchWindow>`. The C# logic smoothly routes the event to the correct active window based on the kickoff time, preventing the "Window Thrashing" bug.

### Cons
1. **Manual Idempotency Gate:** Orleans is not a stream processing engine. It does not have built-in "Watermarks." We had to manually construct a **Per-Provider High Water Mark** gate (`event.EventTime <= ProviderHighWaterMarks[provider]`) inside the Grain to safely reject out-of-order data.
2. **At-Least-Once Delivery:** Unlike Flink's Exactly-Once, if the Orleans node crashes exactly *after* writing to MongoDB but *before* returning the RPC call to the Kafka Worker, the Kafka offset is not committed. The Worker will resend the event later. However, because our Grain employs the Idempotency Gate (High-Water Marks) and the Diffing Engine, processing the same message twice yields `0` Deltas, rendering the process functionally idempotent.

### Verdict
The **Orleans (V2)** architecture is vastly superior for this specific use case. The ability to reuse C# Domain models for the strict Diffing Engine (generating specific Deltas for SignalR fan-out) and the seamless integration with MongoDB for both Clustering and State Persistence heavily outweighs the manual effort of implementing the Idempotency Gate.
