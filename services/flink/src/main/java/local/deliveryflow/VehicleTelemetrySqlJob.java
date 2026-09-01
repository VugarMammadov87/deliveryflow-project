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

import java.io.OutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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
        String bootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = getenv("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", "vehicle-telemetry-events");
        String clickhouseUrl = getenv("CLICKHOUSE_URL", "http://clickhouse:8123");
        String clickhouseDatabase = getenv("FLEET_CLICKHOUSE_DATABASE", "fleet");
        String clickhouseUser = getenv("CLICKHOUSE_USER", "delivery_app");
        String clickhousePassword = getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password");
        double overspeedThreshold = Double.parseDouble(getenv("FLEET_OVERSPEED_THRESHOLD_KMH", "100"));
        double lowFuelThreshold = Double.parseDouble(getenv("FLEET_LOW_FUEL_THRESHOLD_PCT", "15"));
        double engineTemperatureThreshold = Double.parseDouble(getenv("FLEET_ENGINE_TEMPERATURE_THRESHOLD_C", "105"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(sourceDdl(bootstrapServers, topic));

        submit(
            tableEnv,
            rawTelemetryQuery(),
            new FleetClickHouseSink(clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword, "vehicle_telemetry_events", FleetRowType.RAW)
        );
        submit(
            tableEnv,
            currentStateQuery(),
            new FleetClickHouseSink(clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword, "vehicle_current_state", FleetRowType.STATE)
        );
        submit(
            tableEnv,
            alertsQuery(overspeedThreshold, lowFuelThreshold, engineTemperatureThreshold),
            new FleetClickHouseSink(clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword, "vehicle_health_alerts", FleetRowType.ALERT)
        );
        submit(
            tableEnv,
            metricsQuery(overspeedThreshold, lowFuelThreshold),
            new FleetClickHouseSink(clickhouseUrl, clickhouseDatabase, clickhouseUser, clickhousePassword, "vehicle_metrics_5m", FleetRowType.METRICS)
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
     * Defines the Kafka-backed Flink SQL source table for telemetry events.
     *
     * <p>The schema mirrors `src/contracts/fleet/vehicle_telemetry_v1.schema.json`.
     * Event time uses `event_timestamp` with a small watermark so the five-minute
     * metrics query can run with proper streaming-time semantics.</p>
     */
    private static String sourceDdl(String bootstrapServers, String topic) {
        return "CREATE TABLE vehicle_telemetry_source ("
            + "schema_version INT,"
            + "event_id STRING,"
            + "event_type STRING,"
            + "event_timestamp TIMESTAMP_LTZ(3),"
            + "ingestion_timestamp TIMESTAMP_LTZ(3),"
            + "vehicle_id STRING,"
            + "driver_id STRING,"
            + "latitude DOUBLE,"
            + "longitude DOUBLE,"
            + "speed_kmh DOUBLE,"
            + "fuel_level_pct DOUBLE,"
            + "engine_temperature_c DOUBLE,"
            + "odometer_km DOUBLE,"
            + "engine_status STRING,"
            + "vehicle_status STRING,"
            + "WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND"
            + ") WITH ("
            + "'connector' = 'kafka',"
            + "'topic' = '" + topic + "',"
            + "'properties.bootstrap.servers' = '" + bootstrapServers + "',"
            + "'properties.group.id' = 'fleet-vehicle-telemetry-sql',"
            + "'scan.startup.mode' = 'earliest-offset',"
            + "'format' = 'json',"
            + "'json.timestamp-format.standard' = 'ISO-8601',"
            + "'json.fail-on-missing-field' = 'false',"
            + "'json.ignore-parse-errors' = 'true'"
            + ")";
    }

    /**
     * Selects valid telemetry events for the immutable history table.
     *
     * <p>This query is intentionally thin: raw history should preserve the event
     * payload after contract and event-type filtering so replay and debugging
     * remain possible.</p>
     */
    private static String rawTelemetryQuery() {
        return "SELECT "
            + "schema_version, event_id, event_type, event_timestamp, ingestion_timestamp, vehicle_id, driver_id, "
            + "latitude, longitude, speed_kmh, fuel_level_pct, engine_temperature_c, odometer_km, engine_status, vehicle_status "
            + "FROM vehicle_telemetry_source WHERE schema_version = 1 AND event_type = 'VEHICLE_TELEMETRY'";
    }

    /**
     * Projects the latest-state fields used by `fleet.vehicle_current_state`.
     *
     * <p>ClickHouse handles latest-row materialization with ReplacingMergeTree
     * and a version column derived from event time, so the stream can emit each
     * observed telemetry state without keeping custom keyed Java state here.</p>
     */
    private static String currentStateQuery() {
        return "SELECT "
            + "vehicle_id, event_id, event_timestamp, driver_id, latitude, longitude, speed_kmh, fuel_level_pct, "
            + "engine_temperature_c, odometer_km, engine_status, vehicle_status "
            + "FROM vehicle_telemetry_source WHERE schema_version = 1 AND event_type = 'VEHICLE_TELEMETRY'";
    }

    /**
     * Builds alert rows from business thresholds configured in `.env`.
     *
     * <p>The CASE expressions keep alert classification visible in SQL, which
     * makes threshold logic easier to audit than burying it in a custom Java
     * mapper. The query emits only abnormal telemetry rows.</p>
     */
    private static String alertsQuery(double overspeedThreshold, double lowFuelThreshold, double engineTemperatureThreshold) {
        return "SELECT event_id, vehicle_id, event_timestamp, "
            + "CASE "
            + "WHEN engine_temperature_c >= " + engineTemperatureThreshold + " THEN 'ENGINE_TEMPERATURE_HIGH' "
            + "WHEN fuel_level_pct <= " + lowFuelThreshold + " THEN 'LOW_FUEL' "
            + "WHEN speed_kmh >= " + overspeedThreshold + " THEN 'OVERSPEED' "
            + "ELSE 'NORMAL' END AS alert_type, "
            + "CASE "
            + "WHEN engine_temperature_c >= " + engineTemperatureThreshold + " THEN 'critical' "
            + "WHEN fuel_level_pct <= " + lowFuelThreshold + " THEN 'warning' "
            + "WHEN speed_kmh >= " + overspeedThreshold + " THEN 'warning' "
            + "ELSE 'info' END AS severity, "
            + "CASE "
            + "WHEN engine_temperature_c >= " + engineTemperatureThreshold + " THEN engine_temperature_c "
            + "WHEN fuel_level_pct <= " + lowFuelThreshold + " THEN fuel_level_pct "
            + "WHEN speed_kmh >= " + overspeedThreshold + " THEN speed_kmh "
            + "ELSE 0.0 END AS observed_value, "
            + "CASE "
            + "WHEN engine_temperature_c >= " + engineTemperatureThreshold + " THEN " + engineTemperatureThreshold + " "
            + "WHEN fuel_level_pct <= " + lowFuelThreshold + " THEN " + lowFuelThreshold + " "
            + "WHEN speed_kmh >= " + overspeedThreshold + " THEN " + overspeedThreshold + " "
            + "ELSE 0.0 END AS threshold "
            + "FROM vehicle_telemetry_source "
            + "WHERE schema_version = 1 AND event_type = 'VEHICLE_TELEMETRY' AND "
            + "(engine_temperature_c >= " + engineTemperatureThreshold + " OR fuel_level_pct <= " + lowFuelThreshold + " OR speed_kmh >= " + overspeedThreshold + ")";
    }

    /**
     * Aggregates telemetry into five-minute operational metrics.
     *
     * <p>This is the reference pattern for future SQL-based stream marts:
     * define a bounded event-time window, group the stream, and write the
     * serving result to a dedicated ClickHouse table.</p>
     */
    private static String metricsQuery(double overspeedThreshold, double lowFuelThreshold) {
        return "SELECT window_start, window_end, "
            + "COUNT(DISTINCT vehicle_id) AS active_vehicle_count, "
            + "AVG(speed_kmh) AS avg_speed_kmh, "
            + "AVG(fuel_level_pct) AS avg_fuel_level_pct, "
            + "MAX(engine_temperature_c) AS max_engine_temperature_c, "
            + "SUM(CASE WHEN speed_kmh >= " + overspeedThreshold + " THEN 1 ELSE 0 END) AS overspeed_vehicle_count, "
            + "SUM(CASE WHEN fuel_level_pct <= " + lowFuelThreshold + " THEN 1 ELSE 0 END) AS low_fuel_vehicle_count "
            + "FROM TABLE(TUMBLE(TABLE vehicle_telemetry_source, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)) "
            + "WHERE schema_version = 1 AND event_type = 'VEHICLE_TELEMETRY' "
            + "GROUP BY window_start, window_end";
    }

    /**
     * Reads runtime configuration without forcing every setting into code.
     */
    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
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
