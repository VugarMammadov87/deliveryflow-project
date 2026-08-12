package local.deliveryflow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class DeliveryStreamingJob {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = getenv("KAFKA_TOPIC", "delivery-events");
        String clickhouseUrl = getenv("CLICKHOUSE_URL", "http://clickhouse:8123");
        String clickhouseDatabase = getenv("CLICKHOUSE_DATABASE", "delivery");
        String clickhouseUser = getenv("CLICKHOUSE_USER", "delivery_app");
        String clickhousePassword = getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId("deliveryflow-flink-current-state")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        WatermarkStrategy<String> watermarks = WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withIdleness(Duration.ofMinutes(1));

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

    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
