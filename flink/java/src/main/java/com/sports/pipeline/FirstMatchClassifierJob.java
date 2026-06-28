package com.sports.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;

import java.time.Duration;

/**
 * Native-Java equivalent of flink/match_pipeline.sql, but with the EXACT +/-2h-from-first semantics
 * (see {@link FirstMatchClassifier}). Wiring: Kafka source -> keyBy(matchKey) -> classifier ->
 * Kafka sinks. The OpenSearch (first only) and MongoDB (deltas) sinks are added the same way using
 * flink-connector-opensearch / flink-connector-mongodb; only the match-deltas sink is shown here to
 * keep the reference focused.
 *
 * <p>Build: {@code mvn -f flink/java/pom.xml package}; submit the shaded jar with {@code flink run}.
 */
public class FirstMatchClassifierJob {

    public static void main(String[] args) throws Exception {
        final String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "redpanda:9092");
        final ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("ingested-events")
                .setGroupId("flink-classifier-java")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<SportEvent> events = env
                .fromSource(source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMinutes(5)),
                        "ingested-events")
                .map(json -> mapper.readValue(json, SportEvent.class))
                .returns(SportEvent.class);

        DataStream<ClassifiedEvent> classified = events
                .keyBy(SportEvent::matchKey)
                .process(new FirstMatchClassifier());

        // match-deltas: keyed by matchId so the SSE service can route by document id.
        KafkaSink<ClassifiedEvent> deltas = KafkaSink.<ClassifiedEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<ClassifiedEvent>builder()
                        .setTopic("match-deltas")
                        .setKeySerializationSchema((ClassifiedEvent c) -> c.matchId.getBytes())
                        .setValueSerializationSchema((ClassifiedEvent c) -> serialize(mapper, c))
                        .build())
                .build();

        classified.sinkTo(deltas);

        // first-match-events (tag A) and not-first-match-events (tag B) are filtered sinks added the
        // same way: classified.filter(c -> c.tag.equals("A")).sinkTo(...); etc.

        env.execute("sports-first-match-classifier-java");
    }

    private static byte[] serialize(ObjectMapper mapper, ClassifiedEvent c) {
        try {
            return mapper.writeValueAsBytes(c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
