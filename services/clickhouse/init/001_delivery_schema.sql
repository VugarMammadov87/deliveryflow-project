-- Operational and BI serving schema for the delivery application.
CREATE DATABASE IF NOT EXISTS delivery;

-- Immutable stream event history written by Flink; partitioned monthly with a 30-day TTL.
CREATE TABLE IF NOT EXISTS delivery.delivery_events
(
    schema_version UInt16,
    event_id String,
    event_type LowCardinality(String),
    event_timestamp DateTime64(3, 'UTC'),
    ingestion_timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
    delivery_id String,
    shipment_id String,
    order_id String,
    vehicle_id String,
    driver_id String,
    route_id String,
    warehouse_id String,
    region String,
    status LowCardinality(String),
    latitude Float64,
    longitude Float64,
    planned_arrival_timestamp Nullable(DateTime64(3, 'UTC')),
    estimated_arrival_timestamp Nullable(DateTime64(3, 'UTC')),
    delay_minutes Int32,
    capacity_used Float64,
    capacity_total Float64,
    service_level LowCardinality(String),
    priority LowCardinality(String),
    customer_id String,
    destination_city String,
    package_count UInt32,
    order_value Float64,
    payment_method LowCardinality(String),
    planned_distance_km Float64,
    traffic_condition LowCardinality(String),
    weather_condition LowCardinality(String),
    raw_event String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (delivery_id, event_timestamp, event_id)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY;

-- Latest operational state per delivery; deduplicated by version using ReplacingMergeTree.
CREATE TABLE IF NOT EXISTS delivery.delivery_current_state
(
    delivery_id String,
    shipment_id String,
    order_id String,
    vehicle_id String,
    driver_id String,
    route_id String,
    warehouse_id String,
    region String,
    status LowCardinality(String),
    latitude Float64,
    longitude Float64,
    planned_arrival_timestamp Nullable(DateTime64(3, 'UTC')),
    estimated_arrival_timestamp Nullable(DateTime64(3, 'UTC')),
    delay_minutes Int32,
    is_delayed UInt8,
    capacity_used Float64,
    capacity_total Float64,
    utilization_ratio Float64,
    service_level LowCardinality(String),
    priority LowCardinality(String),
    customer_id String,
    destination_city String,
    package_count UInt32,
    order_value Float64,
    payment_method LowCardinality(String),
    planned_distance_km Float64,
    traffic_condition LowCardinality(String),
    weather_condition LowCardinality(String),
    last_event_id String,
    last_event_timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY delivery_id;

-- Latest operational state and location per active delivery vehicle.
CREATE TABLE IF NOT EXISTS delivery.vehicle_current_state
(
    vehicle_id String,
    delivery_id String,
    route_id String,
    latitude Float64,
    longitude Float64,
    active_status LowCardinality(String),
    capacity_used Float64,
    capacity_total Float64,
    utilization_ratio Float64,
    traffic_condition LowCardinality(String),
    weather_condition LowCardinality(String),
    last_event_timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY vehicle_id;

-- Daily logistics KPIs published by the Spark batch job; queried by Superset reporting.
CREATE TABLE IF NOT EXISTS delivery.daily_delivery_kpi
(
    business_date Date,
    region String,
    warehouse_id String,
    completed_deliveries UInt64,
    delayed_deliveries UInt64,
    on_time_deliveries UInt64,
    delay_rate Float64,
    on_time_rate Float64,
    avg_delay_minutes Float64,
    avg_vehicle_utilization Float64,
    total_order_value Float64,
    total_package_count UInt64,
    avg_distance_km Float64,
    published_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, region, warehouse_id);

-- Daily transportation cost KPIs published by the Spark transportation cost batch job.
-- The route-level grain supports management reporting by date, warehouse, region, and route.
CREATE TABLE IF NOT EXISTS delivery.daily_transportation_cost_kpi
(
    business_date Date,
    warehouse_id String,
    region String,
    route_id String,
    cost_record_count UInt64,
    total_deliveries UInt64,
    planned_cost Float64,
    actual_cost Float64,
    fuel_cost Float64,
    driver_cost Float64,
    toll_cost Float64,
    maintenance_cost Float64,
    other_cost Float64,
    total_distance_km Float64,
    avg_vehicle_utilization_pct Float64,
    avg_delivery_duration_minutes Float64,
    avg_duration_variance_minutes Float64,
    cost_variance Float64,
    cost_variance_pct Float64,
    cost_per_km Float64,
    cost_per_delivery Float64,
    fuel_cost_pct Float64,
    published_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, warehouse_id, region, route_id);

-- BI View 1: Delivery status distribution and average delay across all events.
CREATE VIEW IF NOT EXISTS delivery.v_delivery_status_overview AS
SELECT
    status,
    count() AS event_count,
    uniqExact(delivery_id) AS delivery_count,
    avg(delay_minutes) AS avg_delay_minutes
FROM delivery.delivery_events
GROUP BY status;

-- BI View 2: Regional performance and delay rates segmented by service level.
CREATE VIEW IF NOT EXISTS delivery.v_delay_by_region AS
SELECT
    region,
    service_level,
    count() AS event_count,
    avg(delay_minutes) AS avg_delay_minutes,
    sum(delay_minutes > 0) / greatest(count(), 1) AS delay_rate
FROM delivery.delivery_events
GROUP BY region, service_level;

-- BI View 3: Vehicle fleet utilization and active route status.
CREATE VIEW IF NOT EXISTS delivery.v_vehicle_utilization AS
SELECT
    vehicle_id,
    anyLast(active_status) AS active_status,
    anyLast(route_id) AS route_id,
    avg(utilization_ratio) AS avg_utilization_ratio,
    max(last_event_timestamp) AS last_seen_at
FROM delivery.vehicle_current_state
GROUP BY vehicle_id;

-- BI View 4: Warehouse and regional operational KPI snapshot.
CREATE VIEW IF NOT EXISTS delivery.v_warehouse_daily_kpi AS
SELECT
    business_date,
    region,
    warehouse_id,
    completed_deliveries,
    delayed_deliveries,
    on_time_deliveries,
    delay_rate,
    on_time_rate,
    avg_vehicle_utilization,
    total_order_value,
    total_package_count,
    avg_distance_km
FROM delivery.daily_delivery_kpi;

-- BI View 5: Hourly delivery event and unique delivery volume trends.
CREATE VIEW IF NOT EXISTS delivery.v_delivery_event_volume AS
SELECT
    toStartOfHour(event_timestamp) AS event_hour,
    event_type,
    region,
    count() AS event_count,
    uniqExact(delivery_id) AS delivery_count
FROM delivery.delivery_events
GROUP BY event_hour, event_type, region;

-- BI View 6: Transportation cost and route performance for management reporting.
-- This view keeps Superset on ClickHouse while combining batch cost KPIs with
-- operational delay signals from the delivery event history.
CREATE VIEW IF NOT EXISTS delivery.v_transportation_cost_performance AS
SELECT
    k.business_date,
    k.warehouse_id,
    k.region,
    k.route_id,
    k.cost_record_count,
    k.total_deliveries,
    k.planned_cost,
    k.actual_cost,
    k.cost_variance,
    k.cost_variance_pct,
    k.fuel_cost,
    k.driver_cost,
    k.toll_cost,
    k.maintenance_cost,
    k.other_cost,
    k.total_distance_km,
    k.cost_per_km,
    k.cost_per_delivery,
    k.fuel_cost_pct,
    k.avg_vehicle_utilization_pct,
    k.avg_delivery_duration_minutes,
    k.avg_duration_variance_minutes,
    coalesce(d.operational_delivery_count, 0) AS operational_delivery_count,
    coalesce(d.delayed_delivery_count, 0) AS delayed_delivery_count,
    coalesce(d.avg_delay_minutes, 0) AS avg_delay_minutes,
    coalesce(d.delay_rate, 0) AS delay_rate,
    k.cost_variance_pct + coalesce(d.delay_rate, 0) AS cost_delay_risk_score,
    k.published_at,
    k.version
FROM delivery.daily_transportation_cost_kpi FINAL AS k
LEFT JOIN
(
    SELECT
        toDate(event_timestamp) AS business_date,
        warehouse_id,
        region,
        route_id,
        uniqExact(delivery_id) AS operational_delivery_count,
        sum(delay_minutes > 0) AS delayed_delivery_count,
        avg(delay_minutes) AS avg_delay_minutes,
        sum(delay_minutes > 0) / greatest(count(), 1) AS delay_rate
    FROM delivery.delivery_events
    GROUP BY business_date, warehouse_id, region, route_id
) AS d
ON k.business_date = d.business_date
    AND k.warehouse_id = d.warehouse_id
    AND k.region = d.region
    AND k.route_id = d.route_id;

-- Operational serving schema for the independent fleet vehicle telemetry application.
CREATE DATABASE IF NOT EXISTS fleet;

-- Raw vehicle telemetry events received from Kafka; partitioned monthly with a 30-day TTL.
CREATE TABLE IF NOT EXISTS fleet.vehicle_telemetry_events
(
    schema_version UInt16,
    event_id String,
    event_type LowCardinality(String),
    event_timestamp DateTime64(3, 'UTC'),
    ingestion_timestamp DateTime64(3, 'UTC'),
    vehicle_id String,
    driver_id String,
    latitude Float64,
    longitude Float64,
    speed_kmh Float64,
    fuel_level_pct Float64,
    engine_temperature_c Float64,
    odometer_km Float64,
    engine_status LowCardinality(String),
    vehicle_status LowCardinality(String),
    processed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (vehicle_id, event_timestamp, event_id)
TTL toDateTime(event_timestamp) + INTERVAL 30 DAY;

-- Latest reported telemetric state and location per fleet vehicle.
CREATE TABLE IF NOT EXISTS fleet.vehicle_current_state
(
    vehicle_id String,
    event_id String,
    last_event_timestamp DateTime64(3, 'UTC'),
    driver_id String,
    latitude Float64,
    longitude Float64,
    speed_kmh Float64,
    fuel_level_pct Float64,
    engine_temperature_c Float64,
    odometer_km Float64,
    engine_status LowCardinality(String),
    vehicle_status LowCardinality(String),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY vehicle_id;

-- Critical and warning health events (overspeed, low fuel, high engine temperature).
CREATE TABLE IF NOT EXISTS fleet.vehicle_health_alerts
(
    event_id String,
    vehicle_id String,
    event_timestamp DateTime64(3, 'UTC'),
    alert_type LowCardinality(String),
    severity LowCardinality(String),
    observed_value Float64,
    threshold Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (alert_type, vehicle_id, event_timestamp, event_id);

-- Aggregated 5-minute tumbling window metrics computed by Flink SQL.
CREATE TABLE IF NOT EXISTS fleet.vehicle_metrics_5m
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    active_vehicle_count UInt64,
    avg_speed_kmh Float64,
    avg_fuel_level_pct Float64,
    max_engine_temperature_c Float64,
    overspeed_vehicle_count UInt64,
    low_fuel_vehicle_count UInt64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(window_start)
ORDER BY window_start;
