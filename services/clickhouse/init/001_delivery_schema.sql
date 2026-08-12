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
    published_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, region, warehouse_id);
