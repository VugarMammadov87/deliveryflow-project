package local.deliveryflow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Independent fleet telemetry streaming application implemented with Flink SQL.
 *
 * <p>This application intentionally runs next to the existing delivery
 * DataStream job instead of replacing it. It demonstrates the multi-application
 * model requested by the scalability prompt: a separate Kafka topic, separate
 * event contract, separate Flink job name, separate ClickHouse schema, and SQL
 * transformations that can become the reference pattern for future domains.</p>
 */
public class VehicleTelemetrySqlJob {
    private static final String SQL_RESOURCE = "/sql/fleet/vehicle_telemetry.sql";
    private static final Set<String> SQL_SECTIONS = Collections.unmodifiableSet(
        Arrays.stream(new String[] {"SOURCE", "RAW TELEMETRY", "CURRENT STATE", "HEALTH ALERTS", "5 MINUTE METRICS"})
            .collect(Collectors.toSet())
    );
    private static final DateTimeFormatter CLICKHOUSE_TIMESTAMP_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Bootstraps the fleet telemetry application from environment variables.
     *
     * <p>The delivery application keeps its own DataStream main class. This
     * method wires only the fleet SQL/Table API pipeline: Kafka source DDL,
     * SQL transformations, ClickHouse sinks, and the Flink job name that the
     * Makefile submitter uses for idempotent runtime checks.</p>
     */
    public static void main(String[] args) throws Exception {
        PlatformConfig platform = PlatformConfig.load();
        PlatformConfig.ClickHouse clickhouse = platform.clickHouse("fleet");
        String bootstrapServers = platform.kafkaBootstrapServers();
        String topic = platform.vehicleTelemetryTopic();
        double overspeedThreshold = Double.parseDouble(getenv("FLEET_OVERSPEED_THRESHOLD_KMH", "100"));
        double lowFuelThreshold = Double.parseDouble(getenv("FLEET_LOW_FUEL_THRESHOLD_PCT", "15"));
        double engineTemperatureThreshold = Double.parseDouble(getenv("FLEET_ENGINE_TEMPERATURE_THRESHOLD_C", "105"));
        Map<String, String> sql = loadSqlSections(SQL_RESOURCE);
        Map<String, String> variables = new LinkedHashMap<>();
        variables.put("KAFKA_BOOTSTRAP_SERVERS", bootstrapServers);
        variables.put("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", topic);
        variables.put("FLEET_OVERSPEED_THRESHOLD_KMH", Double.toString(overspeedThreshold));
        variables.put("FLEET_LOW_FUEL_THRESHOLD_PCT", Double.toString(lowFuelThreshold));
        variables.put("FLEET_ENGINE_TEMPERATURE_THRESHOLD_C", Double.toString(engineTemperatureThreshold));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(render(section(sql, "SOURCE"), variables));

        submit(
            tableEnv,
            section(sql, "RAW TELEMETRY"),
            new FleetClickHouseSink(clickhouse.url, clickhouse.database, clickhouse.user, clickhouse.password, "vehicle_telemetry_events", FleetRowType.RAW)
        );
        submit(
            tableEnv,
            section(sql, "CURRENT STATE"),
            new FleetClickHouseSink(clickhouse.url, clickhouse.database, clickhouse.user, clickhouse.password, "vehicle_current_state", FleetRowType.STATE)
        );
        submit(
            tableEnv,
            render(section(sql, "HEALTH ALERTS"), variables),
            new FleetClickHouseSink(clickhouse.url, clickhouse.database, clickhouse.user, clickhouse.password, "vehicle_health_alerts", FleetRowType.ALERT)
        );
        submit(
            tableEnv,
            render(section(sql, "5 MINUTE METRICS"), variables),
            new FleetClickHouseSink(clickhouse.url, clickhouse.database, clickhouse.user, clickhouse.password, "vehicle_metrics_5m", FleetRowType.METRICS)
        );

        env.execute("fleet-vehicle-telemetry-sql");
    }

    /**
     * Converts one SQL query into a changelog stream and attaches its matching
     * ClickHouse sink.
     *
     * <p>The helper keeps the main method declarative: every output table is
     * represented by one SQL query and one sink, which is the pattern future
     * fleet metrics can reuse without adding another Flink application.</p>
     */
    private static void submit(StreamTableEnvironment tableEnv, String sql, FleetClickHouseSink sink) {
        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> rows = tableEnv.toChangelogStream(table);
        rows.addSink(sink).name(sink.name());
    }

    /**
     * Reads runtime configuration without forcing every setting into code.
     */
    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    /**
     * Loads named SQL sections from the application SQL resource.
     */
    private static Map<String, String> loadSqlSections(String resourcePath) throws Exception {
        InputStream stream = VehicleTelemetrySqlJob.class.getResourceAsStream(resourcePath);
        if (stream == null) {
            throw new IllegalStateException("SQL resource not found: " + resourcePath);
        }

        Map<String, StringBuilder> builders = new LinkedHashMap<>();
        String currentSection = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.startsWith("-- ")) {
                    String section = trimmed.substring(3).trim().toUpperCase(Locale.ROOT);
                    if (SQL_SECTIONS.contains(section)) {
                        currentSection = section;
                        builders.putIfAbsent(currentSection, new StringBuilder());
                        continue;
                    }
                }
                if (currentSection != null) {
                    builders.get(currentSection).append(line).append('\n');
                }
            }
        }

        Map<String, String> sections = new LinkedHashMap<>();
        for (Map.Entry<String, StringBuilder> entry : builders.entrySet()) {
            sections.put(entry.getKey(), stripTrailingSemicolon(entry.getValue().toString()));
        }
        return sections;
    }

    private static String section(Map<String, String> sections, String name) {
        String sql = sections.get(name);
        if (sql == null || sql.isBlank()) {
            throw new IllegalStateException("Missing SQL section: " + name);
        }
        return sql;
    }

    private static String render(String sql, Map<String, String> variables) {
        String rendered = sql;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            rendered = rendered.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return rendered;
    }

    private static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private enum FleetRowType {
        RAW,
        STATE,
        ALERT,
        METRICS
    }

    /**
     * Minimal ClickHouse HTTP sink used by the local fleet reference app.
     *
     * <p>The existing delivery app already has a dedicated sink tuned for its
     * DataStream records. Fleet SQL queries produce generic `Row` objects, so
     * this sink converts each output shape into JSONEachRow and posts it to the
     * matching ClickHouse table. Keeping the sink local to this class makes the
     * example easy to inspect while the platform is still a Docker Compose lab.</p>
     */
    private static final class FleetClickHouseSink extends RichSinkFunction<Row> {
        private final String clickhouseUrl;
        private final String database;
        private final String user;
        private final String password;
        private final String table;
        private final FleetRowType rowType;

        private FleetClickHouseSink(String clickhouseUrl, String database, String user, String password, String table, FleetRowType rowType) {
            this.clickhouseUrl = clickhouseUrl;
            this.database = database;
            this.user = user;
            this.password = password;
            this.table = table;
            this.rowType = rowType;
        }

        private String name() {
            return "clickhouse-" + table + "-sink";
        }

        @Override
        public void open(Configuration parameters) {
            // The local reference app keeps HTTP inserts simple and inspectable.
        }

        @Override
        public void invoke(Row row, Context context) throws Exception {
            if (row.getKind() != RowKind.INSERT && row.getKind() != RowKind.UPDATE_AFTER) {
                return;
            }
            String json;
            if (rowType == FleetRowType.RAW) {
                json = rawJson(row);
            } else if (rowType == FleetRowType.STATE) {
                json = stateJson(row);
            } else if (rowType == FleetRowType.ALERT) {
                json = alertJson(row);
            } else {
                json = metricsJson(row);
            }
            postSql("INSERT INTO " + database + "." + table + " FORMAT JSONEachRow\n" + json);
        }

        /**
         * Sends one JSONEachRow insert request to ClickHouse.
         *
         * <p>HTTP is enough for this local reference implementation and avoids
         * introducing another JDBC sink dependency while the SQL job pattern is
         * being established.</p>
         */
        private void postSql(String sql) throws Exception {
            HttpURLConnection connection = (HttpURLConnection) URI.create(clickhouseUrl + "/?user=" + user + "&password=" + password).toURL().openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            byte[] payload = sql.getBytes(StandardCharsets.UTF_8);
            connection.setFixedLengthStreamingMode(payload.length);
            try (OutputStream stream = connection.getOutputStream()) {
                stream.write(payload);
            }
            int code = connection.getResponseCode();
            if (code < 200 || code >= 300) {
                InputStream errorStream = connection.getErrorStream();
                String body = errorStream == null ? "" : new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
                throw new IllegalStateException(
                    "ClickHouse fleet insert failed with HTTP " + code + " for table " + table
                        + ": " + body + " SQL=" + sql
                );
            }
        }

        /**
         * Serializes the raw telemetry query output into ClickHouse JSONEachRow.
         */
        private String rawJson(Row row) {
            return "{"
                + "\"schema_version\":" + value(row, 0) + ","
                + "\"event_id\":" + quote(value(row, 1)) + ","
                + "\"event_type\":" + quote(value(row, 2)) + ","
                + "\"event_timestamp\":" + quote(timestamp(row, 3)) + ","
                + "\"ingestion_timestamp\":" + quote(timestamp(row, 4)) + ","
                + "\"vehicle_id\":" + quote(value(row, 5)) + ","
                + "\"driver_id\":" + quote(value(row, 6)) + ","
                + "\"latitude\":" + value(row, 7) + ","
                + "\"longitude\":" + value(row, 8) + ","
                + "\"speed_kmh\":" + value(row, 9) + ","
                + "\"fuel_level_pct\":" + value(row, 10) + ","
                + "\"engine_temperature_c\":" + value(row, 11) + ","
                + "\"odometer_km\":" + value(row, 12) + ","
                + "\"engine_status\":" + quote(value(row, 13)) + ","
                + "\"vehicle_status\":" + quote(value(row, 14))
                + "}";
        }

        /**
         * Serializes latest vehicle state rows and derives the ReplacingMergeTree
         * version from event time.
         */
        private String stateJson(Row row) {
            String eventTimestamp = timestamp(row, 2);
            return "{"
                + "\"vehicle_id\":" + quote(value(row, 0)) + ","
                + "\"event_id\":" + quote(value(row, 1)) + ","
                + "\"last_event_timestamp\":" + quote(eventTimestamp) + ","
                + "\"driver_id\":" + quote(value(row, 3)) + ","
                + "\"latitude\":" + value(row, 4) + ","
                + "\"longitude\":" + value(row, 5) + ","
                + "\"speed_kmh\":" + value(row, 6) + ","
                + "\"fuel_level_pct\":" + value(row, 7) + ","
                + "\"engine_temperature_c\":" + value(row, 8) + ","
                + "\"odometer_km\":" + value(row, 9) + ","
                + "\"engine_status\":" + quote(value(row, 10)) + ","
                + "\"vehicle_status\":" + quote(value(row, 11)) + ","
                + "\"version\":" + version(eventTimestamp)
                + "}";
        }

        /**
         * Serializes fleet health alert rows produced by threshold SQL.
         */
        private String alertJson(Row row) {
            return "{"
                + "\"event_id\":" + quote(value(row, 0)) + ","
                + "\"vehicle_id\":" + quote(value(row, 1)) + ","
                + "\"event_timestamp\":" + quote(timestamp(row, 2)) + ","
                + "\"alert_type\":" + quote(value(row, 3)) + ","
                + "\"severity\":" + quote(value(row, 4)) + ","
                + "\"observed_value\":" + value(row, 5) + ","
                + "\"threshold\":" + value(row, 6)
                + "}";
        }

        /**
         * Serializes the five-minute metric window output.
         */
        private String metricsJson(Row row) {
            String windowStart = timestamp(row, 0);
            return "{"
                + "\"window_start\":" + quote(windowStart) + ","
                + "\"window_end\":" + quote(timestamp(row, 1)) + ","
                + "\"active_vehicle_count\":" + value(row, 2) + ","
                + "\"avg_speed_kmh\":" + value(row, 3) + ","
                + "\"avg_fuel_level_pct\":" + value(row, 4) + ","
                + "\"max_engine_temperature_c\":" + value(row, 5) + ","
                + "\"overspeed_vehicle_count\":" + value(row, 6) + ","
                + "\"low_fuel_vehicle_count\":" + value(row, 7) + ","
                + "\"version\":" + version(windowStart)
                + "}";
        }

        /**
         * Reads a Flink Row field as a ClickHouse-compatible scalar string.
         */
        private static String value(Row row, int index) {
            Object value = row.getField(index);
            return value == null ? "" : value.toString();
        }

        /**
         * Normalizes Flink timestamp field types to the ClickHouse DateTime text
         * shape accepted by JSONEachRow inserts.
         */
        private static String timestamp(Row row, int index) {
            Object value = row.getField(index);
            if (value instanceof Instant) {
                return CLICKHOUSE_TIMESTAMP_FORMATTER.format(LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC));
            }
            if (value instanceof LocalDateTime) {
                return CLICKHOUSE_TIMESTAMP_FORMATTER.format((LocalDateTime) value);
            }
            if (value == null) {
                return "1970-01-01 00:00:00.000";
            }
            String raw = value.toString().replace("T", " ").replace("Z", "");
            LocalDateTime parsed = LocalDateTime.parse(raw.replace(" ", "T"));
            return CLICKHOUSE_TIMESTAMP_FORMATTER.format(parsed);
        }

        /**
         * Converts event time into an epoch-millisecond version for replacing
         * latest-state tables.
         */
        private static long version(String timestamp) {
            String normalized = timestamp.replace(" ", "T");
            return LocalDateTime.parse(normalized).toInstant(ZoneOffset.UTC).toEpochMilli();
        }

        /**
         * Escapes JSON string values before composing JSONEachRow payloads.
         */
        private static String quote(String value) {
            return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
    }
}
