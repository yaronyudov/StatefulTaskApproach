# Flink stateful classification

Two equivalent implementations of the first-vs-not-first stage:

| File | Semantics | Notes |
|------|-----------|-------|
| `match_pipeline.sql` | **Relaxed** (gap-from-previous > 2h = first) | Shipped. Pure Flink SQL, per-event, instant. All I/O declarative. |
| `java/.../FirstMatchClassifier.java` | **Exact** (±2h from the window's first event) | Native Java `KeyedProcessFunction` with keyed anchor state + TTL. |

## Why two?

Flink SQL `MATCH_RECOGNIZE` is **`ONE ROW PER MATCH` only**, and SQL cannot maintain/reset a per-key
running "anchor", so the exact "±2h from the *first* event of the window" rule (with a per-event
delta output and an anchor-based `matchId`) is **not expressible in pure SQL**. The SQL job therefore
uses a gap-from-previous approximation; the Java job implements the precise spec.

## Run the SQL job (local)

The Flink image needs the Kafka, OpenSearch and MongoDB SQL connector JARs on its classpath
(mounted into `/opt/flink/lib` by `deploy/docker-compose.yml`). Then:

```bash
docker compose -f deploy/docker-compose.yml exec jobmanager \
  ./bin/sql-client.sh -f /opt/flink/sql/match_pipeline.sql
```

## Run the Java job (local)

```bash
mvn -f flink/java/pom.xml package
docker compose -f deploy/docker-compose.yml exec jobmanager \
  ./bin/flink run /opt/flink/usrlib/first-match-classifier-1.0.0.jar
```

## AWS

Both run unchanged on **Amazon Managed Service for Apache Flink** (SQL via a Studio notebook /
packaged SQL; Java as a shaded jar application).
