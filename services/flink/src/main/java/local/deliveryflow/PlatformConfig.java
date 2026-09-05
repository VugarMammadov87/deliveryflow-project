package local.deliveryflow;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Shared platform connection reader for Flink applications.
 */
public final class PlatformConfig {
    private static final String DEFAULT_CONFIG_PATH = "/opt/deliveryflow/configs/platform.yaml";

    private final Map<String, Object> platform;

    private PlatformConfig(Map<String, Object> platform) {
        this.platform = platform;
    }

    @SuppressWarnings("unchecked")
    public static PlatformConfig load() throws Exception {
        String path = env("DELIVERYFLOW_CONFIG_PATH", env("PLATFORM_CONFIG_PATH", DEFAULT_CONFIG_PATH));
        try (InputStream stream = new FileInputStream(path)) {
            Object loaded = new Yaml().load(stream);
            Map<String, Object> root = (Map<String, Object>) loaded;
            Object platform = root.containsKey("platform") ? root.get("platform") : root;
            return new PlatformConfig((Map<String, Object>) platform);
        }
    }

    public String kafkaBootstrapServers() {
        return env("KAFKA_BOOTSTRAP_SERVERS", text(section("kafka").get("internal_bootstrap_servers")));
    }

    public String deliveryTopic() {
        return env("KAFKA_TOPIC", env("KAFKA_TOPIC_DELIVERY_EVENTS", text(section("kafka", "topics").get("delivery_events"))));
    }

    public String vehicleTelemetryTopic() {
        return env("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", text(section("kafka", "topics").get("vehicle_telemetry_events")));
    }

    public ClickHouse clickHouse(String application) {
        Map<String, Object> clickhouse = section("clickhouse");
        Map<String, Object> databases = section("clickhouse", "databases");
        String databaseKey = "fleet".equals(application) ? "fleet" : "delivery";
        String databaseEnv = "fleet".equals(application) ? "FLEET_CLICKHOUSE_DATABASE" : "CLICKHOUSE_DATABASE";
        return new ClickHouse(
            env("CLICKHOUSE_URL", text(clickhouse.get("http_url"))),
            env(databaseEnv, text(databases.get(databaseKey))),
            env(text(clickhouse.get("username_env")), ""),
            env(text(clickhouse.get("password_env")), "")
        );
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> section(String first, String... rest) {
        Object current = platform.get(first);
        for (String key : rest) {
            current = ((Map<String, Object>) current).get(key);
        }
        return (Map<String, Object>) current;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String text(Object value) {
        return String.valueOf(value);
    }

    public static final class ClickHouse {
        public final String url;
        public final String database;
        public final String user;
        public final String password;

        private ClickHouse(String url, String database, String user, String password) {
            this.url = url;
            this.database = database;
            this.user = user;
            this.password = password;
        }
    }
}
