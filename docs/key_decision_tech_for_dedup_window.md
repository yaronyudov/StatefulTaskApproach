# Architecture Decision Record: Technology for Stateful Dedup Window (+/- 2h Anchor)

## Context
The system requires a strict stateful evaluation of incoming match events: when the *first* event for a `matchKey` arrives, its `startTime` (kickoff) is set as an "Anchor". All subsequent events for that `matchKey` must be evaluated against a strict `[Anchor - 2h, Anchor + 2h]` window. If an event falls outside this window, it establishes a new Anchor and a new match window.

Pure SQL (including Flink SQL) cannot mathematically express this dynamic, unbounded, per-key anchor resetting without drift or data loss (due to limitations with `LAG` and windowing functions). 

This document evaluates the architectural options for executing this stateful logic natively in Java or C#.

---

## Option 1: Apache Flink (Native Java)
*Current Implementation*

Flink natively handles distributed state, timers, offsets, and exactly-once processing guarantees. The logic is written as a `KeyedProcessFunction` in Java.

* **Throughput:** Millions of events per second.
* **Pros:** Blazing fast. State is held in local RocksDB (no network hops to evaluate the +/- 2h rule). Bulletproof crash recovery.
* **Cons:** Forces the team to maintain and deploy a separate Java codebase alongside the .NET microservices.

## Option 2: Flink Stateful Functions (StateFun with C#)
Flink handles all the streaming mechanics (Kafka consumption, state persistence, fault tolerance). For every event, Flink calls a stateless C# ASP.NET Core API via HTTP/gRPC, injecting both the event payload *and* the current anchor state. The C# code evaluates the rule, mutates the state, and returns it to Flink.

* **Throughput:** ~10,000 to 100,000 events per second.
* **Pros:** Retains Flink's exactly-once guarantees and horizontal scaling, but 100% of the business logic is written in C#.
* **Cons:** Slower than pure Java due to the serialization and network hop (HTTP/gRPC) for every single event. Requires maintaining both a Flink cluster and a C# web farm.

## Option 3: Microsoft Orleans (Native C# Actor Model)
Replace Flink entirely with a pure .NET stream processing engine using Microsoft Orleans. Every `matchKey` becomes a "Virtual Actor" in C# memory.

* **Throughput:** Millions of events per second.
* **Pros:** Blazing fast (in-memory evaluation, no network hops to DB). Extremely fault-tolerant. 100% native .NET.
* **Cons:** Steep learning curve. Requires adopting the Actor Model paradigm, drastically changing the application structure.

## Option 4: C# Kafka Consumer + Redis (with Lua Scripts)
A standard C# Background Worker consumes Kafka. It queries a Redis cluster to check/set the Anchor.

* **Throughput:** ~50,000 to 200,000 events per second.
* **Pros:** Fast and simple to implement in a standard ASP.NET Core worker.
* **Cons (Critical Flaw):** Suffers from the "Two-Phase Commit Problem". If the C# app updates Redis but crashes before publishing the result to the output Kafka topic, the system enters split-brain (Redis thinks the event was processed, but Kafka downstream consumers never got it). Fixing this requires complex Transactional Outbox patterns.
