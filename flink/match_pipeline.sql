-- =============================================================================
-- Sports pipeline — Flink SQL job (SHIPPED, relaxed semantics)
--
-- Reads validated domain events from Kafka, classifies each event as the FIRST
-- of a match window ('A') or a follow-up delta ('B'), and fans out to:
--   * first-match-events  (Kafka)   tag = 'A'
--   * not-first-match-events (Kafka) tag = 'B'
--   * match-deltas        (Kafka)   every row  -> consumed by the C# SSE service
--   * OpenSearch index    (first rows only)     -> discovery: "find the match"
--   * MongoDB collection  (every row, upsert)   -> details by matchId (_id)
--
-- SEMANTICS NOTE
-- --------------
-- Flink SQL MATCH_RECOGNIZE is ONE ROW PER MATCH only, and SQL cannot reset a
-- per-key running anchor, so the EXACT "+/-2h from the FIRST event of the window"
-- rule is NOT expressible in pure SQL. This file therefore uses a RELAXED rule:
-- an event is FIRST when there is no previous event for the match key OR the gap
-- to the previous event is > 2h (LAG over the keyed, time-ordered stream).
--
-- The EXACT anchored +/-2h semantics (with an anchor-based matchId) are
-- implemented natively in Java in:
--     flink/java/src/main/java/com/sports/pipeline/FirstMatchClassifier.java
-- Run that job instead of this file when you need the precise spec behaviour.
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
  matchKey || '_' || DATE_FORMAT(CAST(eventTime AS TIMESTAMP(3)), 'yyyyMMdd') AS matchId,
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

CREATE TABLE not_first_match_events (
  matchId STRING, matchKey STRING, sportType STRING, competitionType STRING,
  startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'not-first-match-events',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601'
);

-- match-deltas: keyed by matchId (raw) so the SSE service routes by document id.
CREATE TABLE match_deltas (
  matchId STRING, matchKey STRING, tag STRING,
  sportType STRING, competitionType STRING,
  startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3),
  homeTeam ROW<id STRING, `name` STRING>,
  awayTeam ROW<id STRING, `name` STRING>,
  metadata MAP<STRING, STRING>
) WITH (
  'connector' = 'kafka', 'topic' = 'match-deltas',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'key.format' = 'raw', 'key.fields' = 'matchId',
  'value.format' = 'json', 'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- OpenSearch discovery index — FIRST rows only, one doc per match (upsert by matchId).
-- 'teams' is the home/away-agnostic array the Query API filters on.
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

-- MongoDB details store — every delta upserts the match document (_id = matchId).
CREATE TABLE mongo_matches (
  matchId STRING, matchKey STRING, tag STRING,
  sport STRING, competition STRING, homeTeam STRING, awayTeam STRING,
  startTime TIMESTAMP_LTZ(3), eventTime TIMESTAMP_LTZ(3),
  PRIMARY KEY (matchId) NOT ENFORCED
) WITH (
  'connector' = 'mongodb',
  'uri' = 'mongodb://mongo:27017',
  'database' = 'sports',
  'collection' = 'matches'
);

-- ---------- One STATEMENT SET so every sink shares a single source scan -------
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO first_match_events
    SELECT matchId, matchKey, sportType, competitionType, startTime, eventTime
    FROM classified WHERE tag = 'A';

  INSERT INTO not_first_match_events
    SELECT matchId, matchKey, sportType, competitionType, startTime, eventTime
    FROM classified WHERE tag = 'B';

  INSERT INTO match_deltas
    SELECT matchId, matchKey, tag, sportType, competitionType,
           startTime, eventTime, homeTeam, awayTeam, metadata
    FROM classified;

  INSERT INTO opensearch_matches
    SELECT matchId, matchKey, sportType, competitionType,
           homeTeam.`name`, awayTeam.`name`,
           ARRAY[homeTeam.`name`, awayTeam.`name`],
           startTime, eventTime
    FROM classified WHERE tag = 'A';

  INSERT INTO mongo_matches
    SELECT matchId, matchKey, tag, sportType, competitionType,
           homeTeam.`name`, awayTeam.`name`, startTime, eventTime
    FROM classified;
END;
