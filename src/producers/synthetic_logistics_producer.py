from __future__ import annotations

"""Synthetic logistics data generator for stream and batch demos.

This module exists to give the local DeliveryFlow platform realistic,
repeatable logistics data without depending on an external source system. The
same domain model feeds Kafka events for the streaming path and PostgreSQL
source rows for the batch path, which keeps dashboard and KPI examples coherent.
"""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Any

LOG = logging.getLogger("deliveryflow.producer")


@dataclass(frozen=True)
class ProducerConfig:
    """Runtime settings for Kafka publishing and source seeding.

    The values come from environment variables so Docker Compose, Makefile
    targets, and smoke tests can run the same generator in different modes
    without changing code.
    """

    bootstrap_servers: str
    topic: str
    event_count: int
    interval_seconds: float
    mode: str
    batch_rows: int
    continuous: bool
    continuous_interval_seconds: float

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """Build producer settings from local defaults and container env vars."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "delivery-events"),
            event_count=int(os.getenv("PRODUCER_EVENT_COUNT", "25")),
            interval_seconds=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.2")),
            mode=os.getenv("PRODUCER_MODE", "both").lower(),
            batch_rows=int(os.getenv("PRODUCER_BATCH_ROWS", "40")),
            continuous=os.getenv("PRODUCER_CONTINUOUS", "false").lower() in {"1", "true", "yes"},
            continuous_interval_seconds=float(os.getenv("PRODUCER_CONTINUOUS_INTERVAL_SECONDS", "600")),
        )


@dataclass(frozen=True)
class SourceDbConfig:
    """Connection settings for the PostgreSQL operational source database."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "SourceDbConfig":
        """Read PostgreSQL source settings from environment variables."""
        return cls(
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "15433")),
            dbname=os.getenv("SOURCE_DB_NAME", "logistics_source"),
            user=os.getenv("SOURCE_DB_USER", "source_app"),
            password=os.getenv("SOURCE_DB_PASSWORD", "local-source-password"),
        )


def build_event(index: int, rng: Random) -> dict[str, Any]:
    """Build one versioned logistics event used by Kafka and source seeding.

    The event intentionally includes identifiers, route/vehicle metadata,
    service-level fields, and delay/utilization facts because downstream Flink,
    Spark, ClickHouse, and Superset examples all depend on those dimensions.
    """
    statuses = ["ORDER_LOADED", "VEHICLE_DEPARTED", "IN_TRANSIT", "DELIVERY_DELAYED", "DELIVERED"]
    regions = ["baku", "absheron", "ganja", "sumgait"]
    service_levels = ["standard", "express", "same_day"]
    priority_levels = ["normal", "high", "critical"]
    payment_methods = ["card", "cash", "corporate_account"]
    traffic_conditions = ["low", "medium", "heavy"]
    weather_conditions = ["clear", "windy", "rain"]
    event_type = statuses[index % len(statuses)]
    delivery_number = index % 24
    region = regions[index % len(regions)]
    service_level = service_levels[index % len(service_levels)]
    priority = priority_levels[index % len(priority_levels)]
    traffic = traffic_conditions[(index + 1) % len(traffic_conditions)]
    weather = weather_conditions[(index + 2) % len(weather_conditions)]
    now = datetime.now(UTC).replace(microsecond=0)
    planned_arrival = now + timedelta(minutes=35 + delivery_number)
    traffic_delay = {"low": 0, "medium": 8, "heavy": 18}[traffic]
    weather_delay = {"clear": 0, "windy": 5, "rain": 12}[weather]
    delay_minutes = 20 + delivery_number if event_type == "DELIVERY_DELAYED" else max(0, delivery_number - 8)
    delay_minutes += traffic_delay + weather_delay
    estimated_arrival = planned_arrival + timedelta(minutes=delay_minutes)
    capacity_total = [80.0, 100.0, 120.0, 160.0][delivery_number % 4]
    capacity_used = min(capacity_total, 35.0 + float((delivery_number * 7) % int(capacity_total)))
    package_count = 6 + (delivery_number * 3) % 36
    order_value = round(25.0 + (delivery_number * 17.35) + rng.uniform(0, 40), 2)

    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": now.isoformat().replace("+00:00", "Z"),
        "order_id": f"order-{delivery_number:04d}",
        "shipment_id": f"shipment-{delivery_number:04d}",
        "delivery_id": f"delivery-{delivery_number:04d}",
        "vehicle_id": f"vehicle-{delivery_number % 8:03d}",
        "driver_id": f"driver-{delivery_number % 10:03d}",
        "warehouse_id": f"warehouse-{region}-01",
        "route_id": f"route-{region}-{delivery_number % 6:03d}",
        "source": "synthetic-logistics-producer",
        "payload": {
            "status": event_type,
            "latitude": 40.4093 + rng.uniform(-0.05, 0.05),
            "longitude": 49.8671 + rng.uniform(-0.05, 0.05),
            "region": region,
            "planned_arrival_timestamp": planned_arrival.isoformat().replace("+00:00", "Z"),
            "estimated_arrival_timestamp": estimated_arrival.isoformat().replace("+00:00", "Z"),
            "delay_minutes": delay_minutes,
            "capacity_used": capacity_used,
            "capacity_total": capacity_total,
            "service_level": service_level,
            "priority": priority,
            "customer_id": f"customer-{delivery_number % 18:04d}",
            "destination_city": region.title(),
            "package_count": package_count,
            "order_value": order_value,
            "payment_method": payment_methods[index % len(payment_methods)],
            "planned_distance_km": round(8.5 + (delivery_number * 2.25), 2),
            "traffic_condition": traffic,
            "weather_condition": weather,
        },
    }


def seed_batch_source(row_count: int, rng: Random) -> None:
    """Populate PostgreSQL source tables with coherent logistics entities.

    The batch path needs relational source tables, not only Kafka events. This
    function derives warehouses, vehicles, drivers, orders, shipments, and
    delivery plans from the same generated event so joins remain consistent.
    """
    import psycopg2

    config = SourceDbConfig.from_env()
    connection = psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname, user=config.user, password=config.password)
    try:
        with connection, connection.cursor() as cursor:
            for index in range(row_count):
                event = build_event(index, rng)
                payload = event["payload"]
                cursor.execute(
                    """
                    INSERT INTO warehouses (warehouse_id, warehouse_name, region, city, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (warehouse_id) DO NOTHING
                    """,
                    (event["warehouse_id"], f"{payload['region'].title()} Fulfillment Hub", payload["region"], payload["destination_city"], payload["latitude"], payload["longitude"]),
                )
                cursor.execute(
                    """
                    INSERT INTO vehicles (vehicle_id, plate_number, vehicle_type, capacity_total)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (vehicle_id) DO NOTHING
                    """,
                    (event["vehicle_id"], f"AZ-{index % 90:02d}-{event['vehicle_id'][-3:]}", "van" if index % 2 else "truck", payload["capacity_total"]),
                )
                cursor.execute(
                    """
                    INSERT INTO drivers (driver_id, full_name, phone_number)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (driver_id) DO NOTHING
                    """,
                    (event["driver_id"], f"Driver {index % 10:02d}", f"+99450123{index % 10000:04d}"),
                )
                order_created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=index % 72)
                promised_delivery_at = order_created_at + timedelta(hours=4 + index % 24)
                cursor.execute(
                    """
                    INSERT INTO customer_orders (
                        order_id, customer_id, customer_region, destination_city, destination_latitude,
                        destination_longitude, service_level, priority, package_count, order_value,
                        payment_method, order_created_at, promised_delivery_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                    """,
                    (
                        event["order_id"],
                        payload["customer_id"],
                        payload["region"],
                        payload["destination_city"],
                        payload["latitude"],
                        payload["longitude"],
                        payload["service_level"],
                        payload["priority"],
                        payload["package_count"],
                        payload["order_value"],
                        payload["payment_method"],
                        order_created_at,
                        promised_delivery_at,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO shipments (shipment_id, order_id, warehouse_id, shipment_status)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (shipment_id) DO NOTHING
                    """,
                    (event["shipment_id"], event["order_id"], event["warehouse_id"], event["event_type"]),
                )
                planned_departure = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=index)
                planned_arrival = planned_departure + timedelta(minutes=35 + index % 24)
                cursor.execute(
                    """
                    INSERT INTO delivery_plans (
                        delivery_id, shipment_id, vehicle_id, driver_id, route_id,
                        planned_departure_at, planned_arrival_at, planned_distance_km
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (delivery_id) DO NOTHING
                    """,
                    (
                        event["delivery_id"],
                        event["shipment_id"],
                        event["vehicle_id"],
                        event["driver_id"],
                        event["route_id"],
                        planned_departure,
                        planned_arrival,
                        payload["planned_distance_km"],
                    ),
                )
        LOG.info("seeded %s PostgreSQL source rows", row_count)
    finally:
        connection.close()


def publish_stream_events(config: ProducerConfig, rng: Random) -> None:
    """Publish generated delivery events to Kafka for the Flink stream job.

    The delivery id is used as the Kafka key so events for the same delivery are
    naturally grouped by downstream keyed operators. Continuous mode supports a
    long-running demo producer that keeps Superset charts changing over time.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )

    index = 0
    while config.continuous or index < config.event_count:
        event = build_event(index, rng)
        producer.send(config.topic, key=event["delivery_id"], value=event)
        LOG.info("published event_id=%s type=%s delivery_id=%s", event["event_id"], event["event_type"], event["delivery_id"])
        producer.flush(timeout=30)
        index += 1
        if config.continuous:
            time.sleep(config.continuous_interval_seconds)
        elif index < config.event_count:
            time.sleep(config.interval_seconds)

    producer.flush(timeout=30)
    producer.close(timeout=10)


def main() -> None:
    """Entrypoint used by Docker Compose, Makefile targets, and smoke tests."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = ProducerConfig.from_env()
    rng = Random(42)

    if config.mode not in {"stream", "batch", "both"}:
        raise ValueError("PRODUCER_MODE must be one of: stream, batch, both")

    if config.mode in {"batch", "both"}:
        seed_batch_source(config.batch_rows, rng)

    if config.mode in {"stream", "both"}:
        publish_stream_events(config, rng)


if __name__ == "__main__":
    main()
