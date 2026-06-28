-- =============================================================================
-- Sports pipeline — Flink SQL job (SHIPPED, relaxed semantics)
--
-- Write strategy (addresses the Mongo/Atlas hot-path concern):
--   * OpenSearch (discovery)  <- FIRST event of a match, written IMMEDIATELY so a
--                                live match is findable right away. One small doc per match.
--   * MongoDB Atlas (details) <- a SINGLE "final match state" write per window, emitted when
--                                the window CLOSES (~2h of no further events). One write per
--                                match instead of one-per-event => no per-document hot path.
--   * Live in-window updates (the "duplicates") are the LIVE-DATA-UPDATES path -> match-deltas /
--     not-first-match-events for SSE. They are COMMENTED OUT below on purpose: enable them when
--     you want to stream live deltas. Writing them one-by-one to Mongo is exactly the hot path
--     we are avoiding here.
--
-- SEMANTICS NOTE
-- --------------
-- Flink SQL MATCH_RECOGNIZE is ONE ROW PER MATCH only and SQL cannot reset a per-key anchor, so the
-- exact "+/-2h from the FIRST event of the window" rule is NOT expressible in pure SQL. This file
-- uses a RELAXED rule: gap-from-previous > 2h = first (LAG), and a SESSION window (2h gap) for the
-- final-state write. The EXACT anchored +/-2h semantics + a window-close timer that emits the final
-- state in one write are implemented natively in Java in:
--     flink/java/src/main/java/com/sports/pipeline/FirstMatchClassifier.java
-- =============================================================================

SET 'pipeline.name' = 'sports-first-match-classifier';
SET 'execution.runtime-mode' = 'streaming';

-- ---------- Source: validated domain events produced by the scrappers --------
CREATE TABLE ingested_events (
  matchKey         STRING,
  sportType        STRING,
  competitionType  STRING,
  startTime        TIMESTAMP_LTZ(3),
  eventTime        TIMESTAMP_LTZ(3),
  homeTeam         ROW<id STRING, `name` STRING>,
  awayTeam         ROW<id STRING, `name` STRING>,
  metadata         MAP<STRING, STRING>,
  WATERMARK FOR eventTime AS eventTime - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ingested-events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-classifier',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- ---------- Classification view (relaxed: gap-from-previous > 2h = first) -----
CREATE TEMPORARY VIEW classified AS
SELECT
  -- Bucket on startTime (constant for every event of a game) so a game that spans midnight
  -- (e.g. 23:00 -> 01:00) stays under ONE matchId. Bucketing on eventTime would split it.
  matchKey || '_' || DATE_FORMAT(CAST(startTime AS TIMESTAMP(3)), 'yyyyMMdd') AS matchId,
  matchKey, sportType, competitionType, startTime, eventTime,
  homeTeam, awayTeam, metadata,
  CASE
    WHEN prev_time IS NULL OR eventTime > prev_time + INTERVAL '2' HOUR THEN 'A'
    ELSE 'B'
  END AS tag
FROM (
  SELECT
    *,
    LAG(eventTime) OVER (PARTITION BY matchKey ORDER BY eventTime) AS prev_time
  FROM ingested_events
);

-- ---------- Sinks ------------------------------------------------------------
CREATE TABLE first_match_events (
  matchId STRING, matchKey STRING, sportType STRING, competitionType STRING,
  startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'first-match-events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601'
);

-- OpenSearch discovery index — FIRST rows only, written immediately (one doc per match).
CREATE TABLE opensearch_matches (
  matchId STRING, matchKey STRING, sport STRING, competition STRING,
  homeTeam STRING, awayTeam STRING, teams ARRAY<STRING>,
  startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3),
  PRIMARY KEY (matchId) NOT ENFORCED
) WITH (
  'connector' = 'opensearch',
  'hosts' = 'http://opensearch:9200',
  'index' = 'matches'
);

-- MongoDB Atlas details store — ONE final-state document per match window (upsert by _id=matchId).
CREATE TABLE mongo_matches (
  matchId STRING, matchKey STRING, sport STRING, competition STRING,
  homeTeam STRING, awayTeam STRING,
  startTime TIMESTAMP_LTZ(3), lastEventTime TIMESTAMP_LTZ(3), updateCount BIGINT,
  PRIMARY KEY (matchId) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://mongo:27017',          -- AWS: Atlas SRV uri, tls=true, retryWrites=false
  'database' = 'sports',
  'collection' = 'matches'
);

-- ===========================================================================
-- LIVE-DATA-UPDATES PATH (COMMENTED OUT ON PURPOSE)
-- These stream every in-window "duplicate" update for live consumers (SSE). They are the per-event
-- writes that would create the Mongo hot path, so they are disabled by default. Uncomment to enable
-- live delta streaming.
-- ---------------------------------------------------------------------------
-- CREATE TABLE not_first_match_events (
--   matchId STRING, matchKey STRING, sportType STRING, competitionType STRING,
--   startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3)
-- ) WITH (
--   'connector' = 'kafka', 'topic' = 'not-first-match-events',
--   'properties.bootstrap.servers' = 'redpanda:9092',
--   'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601'
-- );
--
-- CREATE TABLE match_deltas (
--   matchId STRING, matchKey STRING, tag STRING,
--   sportType STRING, competitionType STRING,
--   startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3),
--   homeTeam ROW<id STRING, `name` STRING>,
--   awayTeam ROW<id STRING, `name` STRING>,
--   metadata MAP<STRING, STRING>
-- ) WITH (
--   'connector' = 'kafka', 'topic' = 'match-deltas',
--   'properties.bootstrap.servers' = 'redpanda:9092',
--   'key.format' = 'raw', 'key.fields' = 'matchId',
--   'value.format' = 'json', 'value.json.timestamp-format.standard' = 'ISO-8601'
-- );
-- ===========================================================================

-- ---------- Run all active sinks from one source scan -----------------------
EXECUTE STATEMENT SET
BEGIN
  -- New game stored (first occurrence), emitted immediately.
  INSERT INTO first_match_events
    SELECT matchId, matchKey, sportType, competitionType, startTime, eventTime
    FROM classified WHERE tag = 'A';

  -- Discovery index: first row only, immediate so a live match is searchable right away.
  INSERT INTO opensearch_matches
    SELECT matchId, matchKey, sportType, competitionType,
           homeTeam.`name`, awayTeam.`name`,
           ARRAY[homeTeam.`name`, awayTeam.`name`],
           startTime, eventTime
    FROM classified WHERE tag = 'A';

  -- Details: ONE final-state write per match when its 2h session window closes (hot-path safe).
  INSERT INTO mongo_matches
    SELECT
      matchKey || '_' || DATE_FORMAT(CAST(MIN(startTime) AS TIMESTAMP(3)), 'yyyyMMdd') AS matchId,
      matchKey,
      LAST_VALUE(sportType), LAST_VALUE(competitionType),
      LAST_VALUE(homeTeam.`name`), LAST_VALUE(awayTeam.`name`),
      MIN(startTime), MAX(eventTime), COUNT(*)
    FROM ingested_events
    GROUP BY matchKey, SESSION(eventTime, INTERVAL '2' HOUR);

  -- LIVE-DATA-UPDATES PATH (disabled — see commented tables above):
  -- INSERT INTO not_first_match_events
  --   SELECT matchId, matchKey, sportType, competitionType, startTime, eventTime
  --   FROM classified WHERE tag = 'B';
  -- INSERT INTO match_deltas
  --   SELECT matchId, matchKey, tag, sportType, competitionType,
  --          startTime, eventTime, homeTeam, awayTeam, metadata
  --   FROM classified;
END;
