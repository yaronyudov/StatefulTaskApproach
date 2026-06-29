package com.sports.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.http.HttpHost;
import org.opensearch.client.Requests;
import org.opensearch.common.xcontent.XContentType;

import java.time.Duration;

/**
 * Native-Java equivalent of flink/match_pipeline.sql with the EXACT +/-2h-from-first semantics and a
 * window-close final write (see {@link FirstMatchClassifier}).
 *
 * Wiring:
 *   main output            -> first-match-events (new game)         + OpenSearch discovery
 *   FINAL_STATE side output-> Mongo/Atlas (ONE write per window)
 *   LIVE_UPDATES side out  -> match-deltas for SSE   (COMMENTED OUT: this is the live path / hot path)
 *
 * Build: {@code mvn -f flink/java/pom.xml package}; submit with {@code flink run}.
 */
public class FirstMatchClassifierJob {

    public static void main(String[] args) throws Exception {
        final String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "redpanda:9092");
        final int ttlHours = Integer.parseInt(System.getenv().getOrDefault("STATE_TTL_HOURS", "24"));
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

        SingleOutputStreamOperator<ClassifiedEvent> classified = events
                .keyBy(SportEvent::matchKey)
                .process(new FirstMatchClassifier(ttlHours));

        // New game (first occurrence) -> first-match-events topic
        classified.sinkTo(kafka(brokers, "first-match-events", c -> c.matchId, c -> serialize(mapper, c)));

        // OpenSearch discovery -> immediately write the first-match discovery record.
        OpensearchSink<ClassifiedEvent> osSink = new OpensearchSinkBuilder<ClassifiedEvent>()
                .setHosts(new HttpHost(System.getenv().getOrDefault("OPENSEARCH_HOST", "opensearch"), 9200, "http"))
                .setEmitter(
                        (element, context, indexer) -> {
                            indexer.add(
                                    Requests.indexRequest("sports_discovery")
                                            .id(element.matchId)
                                            .source(serialize(mapper, element), XContentType.JSON));
                        })
                .build();
        classified.sinkTo(osSink);

        // FINAL match state -> ONE write per window. Uses Flink MongoSink
        // writing _id=matchId to Atlas using upsert.
        MongoSink<FinalMatchState> mongoSink = MongoSink.<FinalMatchState>builder()
                .setUri(System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017"))
                .setDatabase("sports")
                .setCollection("matches")
                .setSerializationSchema(
                        (input, context) -> {
                            Document doc = Document.parse(new String(serialize(mapper, input)));
                            BsonDocument filter = new BsonDocument("_id", new BsonString(input.matchId));
                            BsonDocument update = new BsonDocument("$set", doc.toBsonDocument());
                            return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
                        }
                )
                .build();

        classified.getSideOutput(FirstMatchClassifier.FINAL_STATE)
                .sinkTo(mongoSink);

        // LIVE-DATA-UPDATES PATH (COMMENTED OUT): per-event in-window deltas for SSE. Enabling this is
        // the per-event write pattern we avoid for the details store; route it to Kafka/SSE only.
        // classified.getSideOutput(FirstMatchClassifier.LIVE_UPDATES)
        //         .sinkTo(kafka(brokers, "match-deltas", c -> c.matchId, c -> serialize(mapper, c)));

        env.execute("sports-first-match-classifier-java");
    }

    private static <T> KafkaSink<T> kafka(String brokers, String topic,
                                          java.util.function.Function<T, String> key,
                                          java.util.function.Function<T, byte[]> value) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema((T t) -> key.apply(t).getBytes())
                        .setValueSerializationSchema(value::apply)
                        .build())
                .build();
    }

    private static byte[] serialize(ObjectMapper mapper, Object o) {
        try {
            return mapper.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
