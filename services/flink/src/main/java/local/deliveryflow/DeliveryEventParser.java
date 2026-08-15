package local.deliveryflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class DeliveryEventParser implements MapFunction<String, DeliveryEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public DeliveryEvent map(String value) throws Exception {
        JsonNode root = MAPPER.readTree(value);
        JsonNode payload = root.path("payload");

        DeliveryEvent event = new DeliveryEvent();
        event.schemaVersion = root.path("schema_version").asInt();
        event.eventId = root.path("event_id").asText();
        event.eventType = root.path("event_type").asText();
        event.eventTimestamp = root.path("event_timestamp").asText();
        event.deliveryId = root.path("delivery_id").asText();
        event.shipmentId = root.path("shipment_id").asText("");
        event.orderId = root.path("order_id").asText("");
        event.vehicleId = root.path("vehicle_id").asText();
        event.driverId = root.path("driver_id").asText("");
        event.warehouseId = root.path("warehouse_id").asText("");
        event.routeId = root.path("route_id").asText("");
        event.region = payload.path("region").asText("");
        event.status = payload.path("status").asText(event.eventType);
        event.latitude = payload.path("latitude").asDouble();
        event.longitude = payload.path("longitude").asDouble();
        event.plannedArrivalTimestamp = payload.path("planned_arrival_timestamp").asText("");
        event.estimatedArrivalTimestamp = payload.path("estimated_arrival_timestamp").asText("");
        event.delayMinutes = payload.path("delay_minutes").asInt();
        event.capacityUsed = payload.path("capacity_used").asDouble();
        event.capacityTotal = payload.path("capacity_total").asDouble();
        event.serviceLevel = payload.path("service_level").asText("");
        event.priority = payload.path("priority").asText("");
        event.customerId = payload.path("customer_id").asText("");
        event.destinationCity = payload.path("destination_city").asText("");
        event.packageCount = payload.path("package_count").asInt();
        event.orderValue = payload.path("order_value").asDouble();
        event.paymentMethod = payload.path("payment_method").asText("");
        event.plannedDistanceKm = payload.path("planned_distance_km").asDouble();
        event.trafficCondition = payload.path("traffic_condition").asText("");
        event.weatherCondition = payload.path("weather_condition").asText("");
        event.rawEvent = value.replace("\\", "\\\\").replace("'", "\\'");
        return event;
    }
}
