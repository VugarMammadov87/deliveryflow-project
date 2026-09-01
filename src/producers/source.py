"""PostgreSQL source seeding for the synthetic logistics batch path."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from random import Random

from producers.config import SourceDbConfig
from producers.events import build_event

LOG = logging.getLogger("deliveryflow.producer")


def seed_batch_source(row_count: int, rng: Random) -> None:
    """Populate PostgreSQL source tables with coherent logistics entities."""
    import psycopg2

    config = SourceDbConfig.from_env()
    connection = psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    )
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
                    (
                        event["warehouse_id"],
                        f"{payload['region'].title()} Fulfillment Hub",
                        payload["region"],
                        payload["destination_city"],
                        payload["latitude"],
                        payload["longitude"],
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vehicles (vehicle_id, plate_number, vehicle_type, capacity_total)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (vehicle_id) DO NOTHING
                    """,
                    (
                        event["vehicle_id"],
                        f"AZ-{index % 90:02d}-{event['vehicle_id'][-3:]}",
                        "van" if index % 2 else "truck",
                        payload["capacity_total"],
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO drivers (driver_id, full_name, phone_number)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (driver_id) DO NOTHING
                    """,
                    (
                        event["driver_id"],
                        f"Driver {index % 10:02d}",
                        f"+99450123{index % 10000:04d}",
                    ),
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
                    (
                        event["shipment_id"],
                        event["order_id"],
                        event["warehouse_id"],
                        event["event_type"],
                    ),
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
