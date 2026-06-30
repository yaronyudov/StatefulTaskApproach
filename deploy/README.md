# Local stack & demo

```bash
# from repo root
docker compose -f deploy/docker-compose.yml up --build
```

Brings up Redpanda (Kafka), Flink (JobManager + TaskManager with connector JARs), OpenSearch,
MongoDB, an nginx mock provider, and the three C# services (scraper, sse, query-api).

## Submit the Flink job

```bash
docker compose -f deploy/docker-compose.yml exec jobmanager \
  ./bin/sql-client.sh -f /opt/flink/sql/match_pipeline.sql
```

(or the exact-semantics Java job — see `flink/README.md`.)

## Watch it work

The scraper polls the mock provider on startup and publishes 5 crafted events to `raw-events`
(the Validator maps + validates them onto `validated-events`, which the classifier consumes):
an Arsenal–Chelsea match with 3 events inside ~80 min (1 first + 2 deltas), a 4th event >2h later
(a NEW first match), and a Lakers–Celtics tip-off.

```bash
# 1) Discovery search (OpenSearch) — any combination of 1..4 params
curl 'http://localhost:8082/matches?sport=Football&team=Arsenal'
curl 'http://localhost:8082/matches?from=2026-06-28T00:00:00Z&to=2026-06-28T23:59:59Z'

# 2) Details by the matchId returned above.
#    Orleans pipeline: live matches are served from Redis (authoritative, current); the QueryApi
#    falls back to MongoDB for ended matches. Mongo is the periodic/at-end archive, not per event.
#    Flink pipeline: details are the FINAL match state, written once the 2h session window closes.
curl 'http://localhost:8082/matches/<matchId>'

# 3) Live deltas over SSE — DISABLED BY DEFAULT.
#    The live-update path (match-deltas) is commented out in flink/match_pipeline.sql (and the Java
#    job) to avoid per-event Mongo writes. Uncomment it to stream deltas, then:
curl -N 'http://localhost:8080/subscribe/<matchId>'
```

## Notes

- The scraper maps provider fields -> domain DTO using `src/SportsPipeline.Scraper/providers.sample.json`.
- **OpenSearch** holds **first-match rows only**, written immediately (discovery / "find the match").
- **Redis** is the **authoritative live store** for in-progress matches (Orleans pipeline): the grain
  writes the current details there before notifying, so a notified user always reads the value that
  triggered the notification. Run it durable (AOF).
- **MongoDB Atlas** is the **archive** (`_id = matchId`): written off the hot path by a periodic
  write-behind flush and once more at match end — never per event. Local compose uses a `mongo:7`
  container as an Atlas stand-in (same wire protocol).
- The **live in-window update path** (SSE deltas) is commented out on purpose; enable it for live data.
- For exact ±2h-from-first semantics run the Java job instead of the SQL (see `flink/README.md`).
