-- SOURCE

CREATE TABLE vehicle_telemetry_source (
    schema_version INT,
    event_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP_LTZ(3),
    ingestion_timestamp TIMESTAMP_LTZ(3),
    vehicle_id STRING,
    driver_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kmh DOUBLE,
    fuel_level_pct DOUBLE,
    engine_temperature_c DOUBLE,
    odometer_km DOUBLE,
    engine_status STRING,
    vehicle_status STRING,
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS}',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'fleet-vehicle-telemetry-sql',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- RAW TELEMETRY

SELECT
    schema_version,
    event_id,
    event_type,
    event_timestamp,
    ingestion_timestamp,
    vehicle_id,
    driver_id,
    latitude,
    longitude,
    speed_kmh,
    fuel_level_pct,
    engine_temperature_c,
    odometer_km,
    engine_status,
    vehicle_status
FROM vehicle_telemetry_source
WHERE schema_version = 1
  AND event_type = 'VEHICLE_TELEMETRY';

-- CURRENT STATE

SELECT
    vehicle_id,
    event_id,
    event_timestamp,
    driver_id,
    latitude,
    longitude,
    speed_kmh,
    fuel_level_pct,
    engine_temperature_c,
    odometer_km,
    engine_status,
    vehicle_status
FROM vehicle_telemetry_source
WHERE schema_version = 1
  AND event_type = 'VEHICLE_TELEMETRY';

-- HEALTH ALERTS

SELECT
    event_id,
    vehicle_id,
    event_timestamp,
    CASE
        WHEN engine_temperature_c >= ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C} THEN 'ENGINE_TEMPERATURE_HIGH'
        WHEN fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT} THEN 'LOW_FUEL'
        WHEN speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH} THEN 'OVERSPEED'
        ELSE 'NORMAL'
    END AS alert_type,
    CASE
        WHEN engine_temperature_c >= ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C} THEN 'critical'
        WHEN fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT} THEN 'warning'
        WHEN speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH} THEN 'warning'
        ELSE 'info'
    END AS severity,
    CASE
        WHEN engine_temperature_c >= ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C} THEN engine_temperature_c
        WHEN fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT} THEN fuel_level_pct
        WHEN speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH} THEN speed_kmh
        ELSE 0.0
    END AS observed_value,
    CASE
        WHEN engine_temperature_c >= ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C} THEN ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C}
        WHEN fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT} THEN ${FLEET_LOW_FUEL_THRESHOLD_PCT}
        WHEN speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH} THEN ${FLEET_OVERSPEED_THRESHOLD_KMH}
        ELSE 0.0
    END AS threshold
FROM vehicle_telemetry_source
WHERE schema_version = 1
  AND event_type = 'VEHICLE_TELEMETRY'
  AND (
      engine_temperature_c >= ${FLEET_ENGINE_TEMPERATURE_THRESHOLD_C}
      OR fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT}
      OR speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH}
  );

-- 5 MINUTE METRICS

SELECT
    window_start,
    window_end,
    COUNT(DISTINCT vehicle_id) AS active_vehicle_count,
    AVG(speed_kmh) AS avg_speed_kmh,
    AVG(fuel_level_pct) AS avg_fuel_level_pct,
    MAX(engine_temperature_c) AS max_engine_temperature_c,
    SUM(CASE WHEN speed_kmh >= ${FLEET_OVERSPEED_THRESHOLD_KMH} THEN 1 ELSE 0 END) AS overspeed_vehicle_count,
    SUM(CASE WHEN fuel_level_pct <= ${FLEET_LOW_FUEL_THRESHOLD_PCT} THEN 1 ELSE 0 END) AS low_fuel_vehicle_count
FROM TABLE(TUMBLE(TABLE vehicle_telemetry_source, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES))
WHERE schema_version = 1
  AND event_type = 'VEHICLE_TELEMETRY'
GROUP BY window_start, window_end;
