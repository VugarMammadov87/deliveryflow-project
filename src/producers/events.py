"""Versioned synthetic event builders for DeliveryFlow applications."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Any


def build_event(index: int, rng: Random) -> dict[str, Any]:
    """Build one versioned logistics event for streaming and source seeding."""
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


def build_vehicle_telemetry_event(index: int, rng: Random) -> dict[str, Any]:
    """Build one fleet telemetry event for the independent fleet application."""
    base_time = datetime.now(UTC).replace(second=0, microsecond=0)
    event_time = base_time + timedelta(minutes=index * 3)
    ingestion_time = event_time + timedelta(seconds=1)
    vehicle_number = 101 + ((index // 4) % 8)
    speed_sequence = [62.0, 108.0, 75.0, 70.0, 95.0, 42.0]
    fuel_sequence = [68.0, 66.0, 14.0, 13.0, 22.0, 49.0]
    temperature_sequence = [88.0, 91.0, 94.0, 108.0, 99.0, 85.0]
    engine_status = "IDLE" if index % 6 == 5 else "RUNNING"
    vehicle_status = "IDLE" if engine_status == "IDLE" else "IN_TRANSIT"

    return {
        "schema_version": 1,
        "event_id": str(uuid.uuid4()),
        "event_type": "VEHICLE_TELEMETRY",
        "event_timestamp": event_time.isoformat().replace("+00:00", "Z"),
        "ingestion_timestamp": ingestion_time.isoformat().replace("+00:00", "Z"),
        "vehicle_id": f"VEH-{vehicle_number:03d}",
        "driver_id": f"DRV-{(vehicle_number % 20) + 1:03d}",
        "latitude": 40.4093 + rng.uniform(-0.08, 0.08),
        "longitude": 49.8671 + rng.uniform(-0.08, 0.08),
        "speed_kmh": speed_sequence[index % len(speed_sequence)],
        "fuel_level_pct": fuel_sequence[index % len(fuel_sequence)],
        "engine_temperature_c": temperature_sequence[index % len(temperature_sequence)],
        "odometer_km": round(145000.0 + (index * 7.8) + rng.uniform(0, 3), 2),
        "engine_status": engine_status,
        "vehicle_status": vehicle_status,
    }
