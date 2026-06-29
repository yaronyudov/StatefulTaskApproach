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

The scraper polls the mock provider on startup and publishes 5 crafted events to `ingested-events`:
an Arsenal–Chelsea match with 3 events inside ~80 min (1 first + 2 deltas), a 4th event >2h later
(a NEW first match), and a Lakers–Celtics tip-off.

```bash
# 1) Discovery search (OpenSearch) — any combination of 1..4 params
curl 'http://localhost:8082/matches?sport=Football&team=Arsenal'
curl 'http://localhost:8082/matches?from=2026-06-28T00:00:00Z&to=2026-06-28T23:59:59Z'

# 2) Details (MongoDB/Atlas) by the matchId returned above.
#    NOTE: the details doc is the FINAL match state, written once the 2h session window CLOSES,
#    so it appears after the window's watermark advances past anchor+2h.
curl 'http://localhost:8082/matches/<matchId>'

# 3) Live deltas over SSE — DISABLED BY DEFAULT.
#    The live-update path (match-deltas) is commented out in flink/match_pipeline.sql (and the Java
#    job) to avoid per-event Mongo writes. Uncomment it to stream deltas, then:
curl -N 'http://localhost:8080/subscribe/<matchId>'
```

## Notes

- The scraper maps provider fields -> domain DTO using `src/SportsPipeline.Scraper/providers.sample.json`.
- **OpenSearch** holds **first-match rows only**, written immediately (discovery / "find the match").
- **MongoDB Atlas** holds **one final-state doc per match window** (`_id = matchId`), written once at
  window close — not per event (avoids the hot path). Local compose uses a `mongo:7` container as an
  Atlas stand-in (same wire protocol).
- The **live in-window update path** (SSE deltas) is commented out on purpose; enable it for live data.
- For exact ±2h-from-first semantics run the Java job instead of the SQL (see `flink/README.md`).
