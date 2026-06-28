package com.sports.pipeline;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * EXACT first-vs-not-first classification — the precise spec semantics that pure Flink SQL cannot
 * express (see flink/match_pipeline.sql for the shipped, relaxed SQL version).
 *
 * <p>Keyed by {@code matchKey}. A per-key {@link ValueState} holds the timestamp of the current
 * window's FIRST event (the anchor). For each event:
 * <ul>
 *   <li>no anchor yet, OR the event is outside [anchor-2h, anchor+2h] -> it is a FIRST match: a new
 *       window opens and the anchor is reset to this event's timestamp;</li>
 *   <li>otherwise it is NOT-FIRST: emitted as a delta within the existing window.</li>
 * </ul>
 * {@code matchId = matchKey + "_" + anchorMillis} so the first event and all its deltas share one id.
 * State TTL bounds growth for keys that go quiet.
 */
public class FirstMatchClassifier extends KeyedProcessFunction<String, SportEvent, ClassifiedEvent> {

    private static final long WINDOW_MILLIS = 2 * 60 * 60 * 1000L; // +/- 2 hours

    private transient ValueState<Long> anchorState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("window-anchor", Long.class);
        // Expire the anchor a while after it stops being updated so dormant matches free state.
        descriptor.enableTimeToLive(StateTtlConfig
                .newBuilder(Time.hours(6))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupFullSnapshot()
                .build());
        anchorState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(SportEvent event, Context ctx, Collector<ClassifiedEvent> out) throws Exception {
        long ts = event.eventTimeMillis();
        Long anchor = anchorState.value();

        boolean isFirst = anchor == null || ts > anchor + WINDOW_MILLIS || ts < anchor - WINDOW_MILLIS;
        if (isFirst) {
            anchor = ts;                 // open a new window anchored at this event
            anchorState.update(anchor);
        }

        String matchId = event.matchKey() + "_" + anchor;
        out.collect(ClassifiedEvent.of(event, matchId, isFirst ? "A" : "B"));
    }
}
