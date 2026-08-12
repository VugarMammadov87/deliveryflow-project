from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Any

from kafka import KafkaProducer

LOG = logging.getLogger("deliveryflow.producer")


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    event_count: int
    interval_seconds: float

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "delivery-events"),
            event_count=int(os.getenv("PRODUCER_EVENT_COUNT", "25")),
            interval_seconds=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.2")),
        )


def build_event(index: int, rng: Random) -> dict[str, Any]:
    statuses = ["ORDER_LOADED", "VEHICLE_DEPARTED", "IN_TRANSIT", "DELIVERY_DELAYED", "DELIVERED"]
    event_type = statuses[index % len(statuses)]
    delivery_number = index % 8
    now = datetime.now(UTC).replace(microsecond=0)
    planned_arrival = now + timedelta(minutes=35 + delivery_number)
    delay_minutes = 20 + delivery_number if event_type == "DELIVERY_DELAYED" else max(0, delivery_number - 3)
    estimated_arrival = planned_arrival + timedelta(minutes=delay_minutes)
    capacity_total = 100.0
    capacity_used = 55.0 + float((delivery_number * 5) % 40)

    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": now.isoformat().replace("+00:00", "Z"),
        "order_id": f"order-{delivery_number:04d}",
        "shipment_id": f"shipment-{delivery_number:04d}",
        "delivery_id": f"delivery-{delivery_number:04d}",
        "vehicle_id": f"vehicle-{delivery_number % 4:03d}",
        "driver_id": f"driver-{delivery_number % 4:03d}",
        "warehouse_id": "warehouse-baku-01",
        "route_id": f"route-{delivery_number % 3:03d}",
        "source": "synthetic-logistics-producer",
        "payload": {
            "status": event_type,
            "latitude": 40.4093 + rng.uniform(-0.05, 0.05),
            "longitude": 49.8671 + rng.uniform(-0.05, 0.05),
            "region": "baku",
            "planned_arrival_timestamp": planned_arrival.isoformat().replace("+00:00", "Z"),
            "estimated_arrival_timestamp": estimated_arrival.isoformat().replace("+00:00", "Z"),
            "delay_minutes": delay_minutes,
            "capacity_used": capacity_used,
            "capacity_total": capacity_total,
        },
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = ProducerConfig.from_env()
    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )
    rng = Random(42)

    for index in range(config.event_count):
        event = build_event(index, rng)
        producer.send(config.topic, key=event["delivery_id"], value=event)
        LOG.info("published event_id=%s type=%s delivery_id=%s", event["event_id"], event["event_type"], event["delivery_id"])
        time.sleep(config.interval_seconds)

    producer.flush(timeout=30)
    producer.close(timeout=10)


if __name__ == "__main__":
    main()
