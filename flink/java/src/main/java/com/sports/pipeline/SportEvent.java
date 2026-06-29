package com.sports.pipeline;

import java.time.Instant;
import java.util.Map;

/**
 * Domain event as produced by the C# scrapers onto the {@code ingested-events} topic. Field names
 * match the JSON (camelCase). Only the bits the classifier needs have behaviour; the rest are
 * carried through to the sinks.
 */
public class SportEvent {
    public String matchKey;
    public String sportType;
    public String competitionType;
    public Instant startTime;
    public Instant eventTime;
    public Team homeTeam;
    public Team awayTeam;
    public Map<String, String> metadata;

    public String matchKey() {
        return matchKey;
    }

    public long eventTimeMillis() {
        return eventTime.toEpochMilli();
    }

    public long startTimeMillis() {
        return startTime.toEpochMilli();
    }

    public static class Team {
        public String id;
        public String name;
    }
}
