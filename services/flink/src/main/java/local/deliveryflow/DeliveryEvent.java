package local.deliveryflow;

import java.io.Serializable;

/**
 * Serializable event model used inside the Flink data stream.
 *
 * <p>The class is intentionally a simple mutable DTO with public fields because
 * Flink serializers and lambdas handle that shape well in a small local job. It
 * mirrors the producer contract and the ClickHouse serving schema so parsing and
 * sink code do not need repeated map lookups.</p>
 */
public class DeliveryEvent implements Serializable {
    /** Event schema version used to reject incompatible producer payloads. */
    public int schemaVersion;
    /** Unique event id used for immutable history and bronze deduplication. */
    public String eventId;
    /** Business event type such as ORDER_LOADED or DELIVERED. */
    public String eventType;
    /** UTC event time from the producer, later used as ClickHouse version time. */
    public String eventTimestamp;
    /** Stable delivery key used for Kafka keys and current-state grouping. */
    public String deliveryId;
    /** Shipment id links delivery events back to source shipment records. */
    public String shipmentId;
    /** Order id links streaming facts to customer order context. */
    public String orderId;
    /** Vehicle id powers vehicle utilization and current-state reports. */
    public String vehicleId;
    /** Driver id is retained for future operational reporting. */
    public String driverId;
    /** Warehouse id is needed for daily KPI grouping. */
    public String warehouseId;
    /** Route id supports route-level operational analysis. */
    public String routeId;
    /** Region is a primary Superset dashboard dimension. */
    public String region;
    /** Latest delivery status used by operational status charts. */
    public String status;
    /** Current latitude used for location/state reporting. */
    public double latitude;
    /** Current longitude used for location/state reporting. */
    public double longitude;
    /** Planned arrival time for ETA and delay analysis. */
    public String plannedArrivalTimestamp;
    /** Estimated arrival time after traffic/weather delay effects. */
    public String estimatedArrivalTimestamp;
    /** Delay amount in minutes, used by delay-rate KPIs. */
    public int delayMinutes;
    /** Used capacity for vehicle utilization calculation. */
    public double capacityUsed;
    /** Total vehicle capacity for utilization calculation. */
    public double capacityTotal;
    /** Service tier dimension for delay analysis. */
    public String serviceLevel;
    /** Priority dimension retained for future escalation views. */
    public String priority;
    /** Customer id allows order/customer correlation. */
    public String customerId;
    /** Destination city is kept for reporting and debugging. */
    public String destinationCity;
    /** Package count is summed in daily KPI outputs. */
    public int packageCount;
    /** Order value is summed in daily KPI outputs. */
    public double orderValue;
    /** Payment method is retained as a possible BI dimension. */
    public String paymentMethod;
    /** Planned distance supports route and distance KPI calculations. */
    public double plannedDistanceKm;
    /** Traffic condition is a delay explanatory dimension. */
    public String trafficCondition;
    /** Weather condition is a delay explanatory dimension. */
    public String weatherCondition;
    /** Escaped original JSON payload stored for audit/debug traceability. */
    public String rawEvent;
}
