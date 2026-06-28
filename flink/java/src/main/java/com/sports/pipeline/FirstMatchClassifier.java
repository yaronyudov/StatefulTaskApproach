package com.sports.pipeline;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * EXACT first-vs-not-first classification with a window-close final write — the precise spec
 * semantics that pure Flink SQL cannot express (see flink/match_pipeline.sql for the relaxed SQL).
 *
 * <p>Keyed by {@code matchKey}. A per-key anchor (the timestamp of the window's FIRST event) is held
 * in state. For each event:
 * <ul>
 *   <li><b>new game</b> — no anchor, OR outside [anchor-2h, anchor+2h]: open a window, emit to the
 *       main output (→ first-match topic + OpenSearch discovery), and register an event-time timer at
 *       {@code anchor+2h};</li>
 *   <li><b>duplicate (live update)</b> — within the window: fold into the running {@link FinalMatchState}
 *       and emit to the {@link #LIVE_UPDATES} side output (SSE). NOT written to Mongo per-event — that
 *       is the hot path we avoid.</li>
 * </ul>
 * When the timer fires (window closed, ~2h later) the aggregated {@link FinalMatchState} is emitted
 * to {@link #FINAL_STATE} as a SINGLE write to Mongo/Atlas.
 */
public class FirstMatchClassifier extends KeyedProcessFunction<String, SportEvent, ClassifiedEvent> {

    private static final long WINDOW_MILLIS = 2 * 60 * 60 * 1000L; // +/- 2 hours

    /** Live in-window updates for SSE (the job leaves this sink commented out by default). */
    public static final OutputTag<ClassifiedEvent> LIVE_UPDATES = new OutputTag<>("live-updates") {};
    /** One aggregated final-state record per window -> single Mongo write. */
    public static final OutputTag<FinalMatchState> FINAL_STATE = new OutputTag<>("final-state") {};

    private transient ValueState<Long> anchorState;
    private transient ValueState<FinalMatchState> accState;

    @Override
    public void open(Configuration parameters) {
        anchorState = getRuntimeContext().getState(new ValueStateDescriptor<>("anchor", Long.class));
        accState = getRuntimeContext().getState(new ValueStateDescriptor<>("acc", FinalMatchState.class));
    }

    @Override
    public void processElement(SportEvent event, Context ctx, Collector<ClassifiedEvent> out) throws Exception {
        long ts = event.eventTimeMillis();
        Long anchor = anchorState.value();

        boolean isFirst = anchor == null || ts > anchor + WINDOW_MILLIS || ts < anchor - WINDOW_MILLIS;
        if (isFirst) {
            anchor = ts;
            anchorState.update(anchor);
            String matchId = event.matchKey() + "_" + anchor;
            accState.update(FinalMatchState.create(event, matchId));
            // Fire one final write when this window closes (event-time).
            ctx.timerService().registerEventTimeTimer(anchor + WINDOW_MILLIS);
            out.collect(ClassifiedEvent.of(event, matchId, "A")); // new game stored (first topic + ES)
        } else {
            String matchId = event.matchKey() + "_" + anchor;
            FinalMatchState acc = accState.value();
            acc.applyUpdate(event);
            accState.update(acc);
            // Live update (duplicate): side output for SSE — NOT a per-event Mongo write.
            ctx.output(LIVE_UPDATES, ClassifiedEvent.of(event, matchId, "B"));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ClassifiedEvent> out) throws Exception {
        FinalMatchState acc = accState.value();
        if (acc != null) {
            ctx.output(FINAL_STATE, acc); // single, final write of the closed window to Mongo/Atlas
        }
        anchorState.clear();
        accState.clear();
    }
}
