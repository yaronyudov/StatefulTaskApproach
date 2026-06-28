package com.sports.pipeline;

import java.time.Instant;
import java.util.Map;

/** A classified event: the original event plus its window {@code matchId} and {@code tag} (A/B). */
public class ClassifiedEvent {
    public String matchId;
    public String matchKey;
    public String tag;            // "A" = first of the window, "B" = not-first (delta)
    public String sportType;
    public String competitionType;
    public Instant startTime;
    public Instant eventTime;
    public SportEvent.Team homeTeam;
    public SportEvent.Team awayTeam;
    public Map<String, String> metadata;

    public static ClassifiedEvent of(SportEvent e, String matchId, String tag) {
        ClassifiedEvent c = new ClassifiedEvent();
        c.matchId = matchId;
        c.matchKey = e.matchKey;
        c.tag = tag;
        c.sportType = e.sportType;
        c.competitionType = e.competitionType;
        c.startTime = e.startTime;
        c.eventTime = e.eventTime;
        c.homeTeam = e.homeTeam;
        c.awayTeam = e.awayTeam;
        c.metadata = e.metadata;
        return c;
    }
}
