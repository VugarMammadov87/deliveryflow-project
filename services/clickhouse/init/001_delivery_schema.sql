CREATE DATABASE IF NOT EXISTS delivery;

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

CREATE VIEW IF NOT EXISTS delivery.v_delivery_status_overview AS
SELECT
    status,
    count() AS event_count,
    uniqExact(delivery_id) AS delivery_count,
    avg(delay_minutes) AS avg_delay_minutes
FROM delivery.delivery_events
GROUP BY status;

CREATE VIEW IF NOT EXISTS delivery.v_delay_by_region AS
SELECT
    region,
    service_level,
    count() AS event_count,
    avg(delay_minutes) AS avg_delay_minutes,
    sum(delay_minutes > 0) / greatest(count(), 1) AS delay_rate
FROM delivery.delivery_events
GROUP BY region, service_level;

CREATE VIEW IF NOT EXISTS delivery.v_vehicle_utilization AS
SELECT
    vehicle_id,
    anyLast(active_status) AS active_status,
    anyLast(route_id) AS route_id,
    avg(utilization_ratio) AS avg_utilization_ratio,
    max(last_event_timestamp) AS last_seen_at
FROM delivery.vehicle_current_state
GROUP BY vehicle_id;

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

CREATE VIEW IF NOT EXISTS delivery.v_delivery_event_volume AS
SELECT
    toStartOfHour(event_timestamp) AS event_hour,
    event_type,
    region,
    count() AS event_count,
    uniqExact(delivery_id) AS delivery_count
FROM delivery.delivery_events
GROUP BY event_hour, event_type, region;

CREATE DATABASE IF NOT EXISTS fleet;

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
