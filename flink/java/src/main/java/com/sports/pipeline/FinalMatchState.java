package com.sports.pipeline;

import java.time.Instant;

/**
 * The aggregated final state of a match window, written to Mongo/Atlas exactly ONCE when the window
 * closes (see {@link FirstMatchClassifier}). Carrying the aggregate (not each event) is what keeps the
 * details store off the per-event hot path.
 */
public class FinalMatchState {
    public String matchId;
    public String matchKey;
    public String sportType;
    public String competitionType;
    public String homeTeam;
    public String awayTeam;
    public Instant startTime;
    public Instant lastEventTime;
    public long updateCount;

    public static FinalMatchState create(SportEvent e, String matchId) {
        FinalMatchState s = new FinalMatchState();
        s.matchId = matchId;
        s.matchKey = e.matchKey;
        s.sportType = e.sportType;
        s.competitionType = e.competitionType;
        s.homeTeam = e.homeTeam.name;
        s.awayTeam = e.awayTeam.name;
        s.startTime = e.startTime;
        s.lastEventTime = e.eventTime;
        s.updateCount = 1;
        return s;
    }

    /** Fold a live in-window update into the running final state. */
    public void applyUpdate(SportEvent e) {
        this.lastEventTime = e.eventTime;
        this.updateCount += 1;
    }
}
