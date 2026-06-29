# Flink stateful classification

Currently, this is implemented strictly in Java to enforce exact ±2h boundaries:

| File | Semantics | Notes |
|------|-----------|-------|
| `java/.../FirstMatchClassifier.java` | **Exact** (±2h from the window's first event) | Native Java `KeyedProcessFunction` with keyed anchor state, window timers, and a configurable TTL (`STATE_TTL_HOURS`). |

## Why Java instead of SQL?

Flink SQL `MATCH_RECOGNIZE` is **`ONE ROW PER MATCH` only**, and SQL cannot maintain/reset a per-key
running "anchor", so the exact "±2h from the *first* event of the window" rule (with a per-event
delta output and an anchor-based `matchId`) is **not expressible in pure SQL**. The Java job implements the precise spec natively, while also providing exactly-once MongoDB and OpenSearch sinks.

## Run the Java job (local)

```bash
mvn -f flink/java/pom.xml package
docker compose -f deploy/docker-compose.yml exec jobmanager \
  ./bin/flink run /opt/flink/usrlib/first-match-classifier-1.0.0.jar
```

## AWS

The Java job runs unchanged on **Amazon Managed Service for Apache Flink** as a shaded jar application. You can configure the state TTL margin by setting `STATE_TTL_HOURS` in the Flink environment properties.
