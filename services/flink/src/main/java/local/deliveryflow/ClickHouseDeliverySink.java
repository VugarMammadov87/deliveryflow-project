package local.deliveryflow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class ClickHouseDeliverySink extends RichSinkFunction<DeliveryEvent> {
    private final String clickhouseUrl;
    private final String database;
    private final String user;
    private final String password;

    public ClickHouseDeliverySink(String clickhouseUrl, String database, String user, String password) {
        this.clickhouseUrl = clickhouseUrl;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) {
        // No persistent connection is kept; this is intentionally simple for local bootstrap.
    }

    @Override
    public void invoke(DeliveryEvent event, Context context) throws Exception {
        long version = Instant.parse(event.eventTimestamp.replace("Z", "Z")).toEpochMilli();
        double utilization = event.capacityTotal > 0 ? event.capacityUsed / event.capacityTotal : 0.0;
        int isDelayed = event.delayMinutes > 0 ? 1 : 0;

        String historySql = "INSERT INTO " + database + ".delivery_events FORMAT JSONEachRow\n" + toHistoryJson(event);
        postSql(historySql);

        String currentSql = "INSERT INTO " + database + ".delivery_current_state FORMAT JSONEachRow\n" + toCurrentJson(event, utilization, isDelayed, version);
        postSql(currentSql);

        String vehicleSql = "INSERT INTO " + database + ".vehicle_current_state FORMAT JSONEachRow\n" + toVehicleJson(event, utilization, version);
        postSql(vehicleSql);
    }

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
            throw new IllegalStateException("ClickHouse insert failed with HTTP " + code);
        }
    }

    private String nullableTimestamp(String value) {
        if (value == null || value.isBlank()) {
            return "null";
        }
        return quote(value.replace("Z", ""));
    }

    private String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private String toHistoryJson(DeliveryEvent event) {
        return "{"
            + "\"schema_version\":" + event.schemaVersion + ","
            + "\"event_id\":" + quote(event.eventId) + ","
            + "\"event_type\":" + quote(event.eventType) + ","
            + "\"event_timestamp\":" + quote(event.eventTimestamp.replace("Z", "")) + ","
            + "\"delivery_id\":" + quote(event.deliveryId) + ","
            + "\"shipment_id\":" + quote(event.shipmentId) + ","
            + "\"order_id\":" + quote(event.orderId) + ","
            + "\"vehicle_id\":" + quote(event.vehicleId) + ","
            + "\"driver_id\":" + quote(event.driverId) + ","
            + "\"route_id\":" + quote(event.routeId) + ","
            + "\"warehouse_id\":" + quote(event.warehouseId) + ","
            + "\"region\":" + quote(event.region) + ","
            + "\"status\":" + quote(event.status) + ","
            + "\"latitude\":" + event.latitude + ","
            + "\"longitude\":" + event.longitude + ","
            + "\"planned_arrival_timestamp\":" + nullableTimestamp(event.plannedArrivalTimestamp) + ","
            + "\"estimated_arrival_timestamp\":" + nullableTimestamp(event.estimatedArrivalTimestamp) + ","
            + "\"delay_minutes\":" + event.delayMinutes + ","
            + "\"capacity_used\":" + event.capacityUsed + ","
            + "\"capacity_total\":" + event.capacityTotal + ","
            + "\"service_level\":" + quote(event.serviceLevel) + ","
            + "\"priority\":" + quote(event.priority) + ","
            + "\"customer_id\":" + quote(event.customerId) + ","
            + "\"destination_city\":" + quote(event.destinationCity) + ","
            + "\"package_count\":" + event.packageCount + ","
            + "\"order_value\":" + event.orderValue + ","
            + "\"payment_method\":" + quote(event.paymentMethod) + ","
            + "\"planned_distance_km\":" + event.plannedDistanceKm + ","
            + "\"traffic_condition\":" + quote(event.trafficCondition) + ","
            + "\"weather_condition\":" + quote(event.weatherCondition) + ","
            + "\"raw_event\":" + quote(event.rawEvent)
            + "}";
    }

    private String toCurrentJson(DeliveryEvent event, double utilization, int isDelayed, long version) {
        return "{"
            + "\"delivery_id\":" + quote(event.deliveryId) + ","
            + "\"shipment_id\":" + quote(event.shipmentId) + ","
            + "\"order_id\":" + quote(event.orderId) + ","
            + "\"vehicle_id\":" + quote(event.vehicleId) + ","
            + "\"driver_id\":" + quote(event.driverId) + ","
            + "\"route_id\":" + quote(event.routeId) + ","
            + "\"warehouse_id\":" + quote(event.warehouseId) + ","
            + "\"region\":" + quote(event.region) + ","
            + "\"status\":" + quote(event.status) + ","
            + "\"latitude\":" + event.latitude + ","
            + "\"longitude\":" + event.longitude + ","
            + "\"planned_arrival_timestamp\":" + nullableTimestamp(event.plannedArrivalTimestamp) + ","
            + "\"estimated_arrival_timestamp\":" + nullableTimestamp(event.estimatedArrivalTimestamp) + ","
            + "\"delay_minutes\":" + event.delayMinutes + ","
            + "\"is_delayed\":" + isDelayed + ","
            + "\"capacity_used\":" + event.capacityUsed + ","
            + "\"capacity_total\":" + event.capacityTotal + ","
            + "\"utilization_ratio\":" + utilization + ","
            + "\"service_level\":" + quote(event.serviceLevel) + ","
            + "\"priority\":" + quote(event.priority) + ","
            + "\"customer_id\":" + quote(event.customerId) + ","
            + "\"destination_city\":" + quote(event.destinationCity) + ","
            + "\"package_count\":" + event.packageCount + ","
            + "\"order_value\":" + event.orderValue + ","
            + "\"payment_method\":" + quote(event.paymentMethod) + ","
            + "\"planned_distance_km\":" + event.plannedDistanceKm + ","
            + "\"traffic_condition\":" + quote(event.trafficCondition) + ","
            + "\"weather_condition\":" + quote(event.weatherCondition) + ","
            + "\"last_event_id\":" + quote(event.eventId) + ","
            + "\"last_event_timestamp\":" + quote(event.eventTimestamp.replace("Z", "")) + ","
            + "\"version\":" + version
            + "}";
    }

    private String toVehicleJson(DeliveryEvent event, double utilization, long version) {
        return "{"
            + "\"vehicle_id\":" + quote(event.vehicleId) + ","
            + "\"delivery_id\":" + quote(event.deliveryId) + ","
            + "\"route_id\":" + quote(event.routeId) + ","
            + "\"latitude\":" + event.latitude + ","
            + "\"longitude\":" + event.longitude + ","
            + "\"active_status\":" + quote(event.status) + ","
            + "\"capacity_used\":" + event.capacityUsed + ","
            + "\"capacity_total\":" + event.capacityTotal + ","
            + "\"utilization_ratio\":" + utilization + ","
            + "\"traffic_condition\":" + quote(event.trafficCondition) + ","
            + "\"weather_condition\":" + quote(event.weatherCondition) + ","
            + "\"last_event_timestamp\":" + quote(event.eventTimestamp.replace("Z", "")) + ","
            + "\"version\":" + version
            + "}";
    }
}
