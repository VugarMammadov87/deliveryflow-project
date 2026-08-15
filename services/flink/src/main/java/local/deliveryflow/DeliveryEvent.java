package local.deliveryflow;

import java.io.Serializable;

public class DeliveryEvent implements Serializable {
    public int schemaVersion;
    public String eventId;
    public String eventType;
    public String eventTimestamp;
    public String deliveryId;
    public String shipmentId;
    public String orderId;
    public String vehicleId;
    public String driverId;
    public String warehouseId;
    public String routeId;
    public String region;
    public String status;
    public double latitude;
    public double longitude;
    public String plannedArrivalTimestamp;
    public String estimatedArrivalTimestamp;
    public int delayMinutes;
    public double capacityUsed;
    public double capacityTotal;
    public String serviceLevel;
    public String priority;
    public String customerId;
    public String destinationCity;
    public int packageCount;
    public double orderValue;
    public String paymentMethod;
    public double plannedDistanceKm;
    public String trafficCondition;
    public String weatherCondition;
    public String rawEvent;
}
