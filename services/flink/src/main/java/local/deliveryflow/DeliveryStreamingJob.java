package local.deliveryflow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Flink streaming entrypoint for the DeliveryFlow real-time path.
 *
 * <p>The job exists to consume versioned delivery events from Kafka, validate
 * the minimum event contract, and publish operational serving state to
 * ClickHouse. Superset reads those ClickHouse tables/views, so this class is the
 * bridge between event transport and dashboard-ready operational data.</p>
 */
public class DeliveryStreamingJob {
    /**
     * Configure Kafka, checkpointing, parsing, validation, and ClickHouse sink.
     *
     * <p>Environment variables keep the same artifact portable between local
     * Docker Compose and any later runtime that uses different service names or
     * credentials.</p>
     */
    public static void main(String[] args) throws Exception {
        String bootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = getenv("KAFKA_TOPIC", "delivery-events");
        String clickhouseUrl = getenv("CLICKHOUSE_URL", "http://clickhouse:8123");
        String clickhouseDatabase = getenv("CLICKHOUSE_DATABASE", "delivery");
        String clickhouseUser = getenv("CLICKHOUSE_USER", "delivery_app");
        String clickhousePassword = getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password");

        /*
         * Checkpointing is enabled because the sink writes operational state.
         * The local lab accepts at-least-once semantics; ClickHouse replacing
         * tables use versions to keep the latest state queryable.
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);

        /*
         * The Kafka source reads from earliest offsets so a newly started local
         * job can replay demo events that were already generated.
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId("deliveryflow-flink-current-state")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        /*
         * Delivery events may arrive slightly out of order. A bounded watermark
         * strategy documents that tolerance even though the current sink writes
         * current-state rows directly.
         */
        WatermarkStrategy<String> watermarks = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withIdleness(Duration.ofMinutes(1));

        /*
         * Parsing is separated from validation so malformed/missing required
         * fields do not silently become dashboard state.
         */
        DataStream<DeliveryEvent> events = env
            .fromSource(source, watermarks, "delivery-events-kafka-source")
            .map(new DeliveryEventParser())
            .name("parse-versioned-json-delivery-events")
            .filter(event -> event.schemaVersion == 1 && event.eventId != null && !event.eventId.isBlank())
            .name("validate-required-v1-fields")
            .keyBy(event -> event.deliveryId);

        events.addSink(new ClickHouseDeliverySink(clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword))
            .name("clickhouse-operational-serving-sink");

        env.execute("deliveryflow-kafka-flink-clickhouse");
    }

    /** Return an environment variable value or a local Docker Compose default. */
    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
