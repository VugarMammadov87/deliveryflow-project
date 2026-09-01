from __future__ import annotations

"""Contract tests for the synthetic delivery event payload.

This test module exists to protect the boundary between the Python producer and
the downstream Java/Flink, ClickHouse, Spark, and Superset pipeline. A small
schema drift in the producer can break multiple services, so the test checks the
fields that the rest of the platform depends on most heavily.
"""

from datetime import datetime
from random import Random

from producers.synthetic_logistics_producer import build_event, build_vehicle_telemetry_event


def test_synthetic_event_uses_versioned_json_contract() -> None:
    """Verify that generated events keep the versioned logistics event shape.

    The test intentionally uses a seeded random generator so failures are about
    contract changes, not nondeterministic sample data. It focuses on required
    identifiers, timestamp format, nested payload existence, and enum-like
    domain values consumed by Flink and ClickHouse.
    """
    event = build_event(0, Random(42))
    assert event["schema_version"] == 1
    assert event["event_id"]
    assert event["event_type"] in {"ORDER_LOADED", "VEHICLE_DEPARTED", "IN_TRANSIT", "DELIVERY_DELAYED", "DELIVERED"}
    assert event["event_timestamp"].endswith("Z")
    assert isinstance(event["payload"], dict)
    assert event["payload"]["capacity_total"] > 0
    assert event["payload"]["package_count"] > 0
    assert event["payload"]["service_level"] in {"standard", "express", "same_day"}
    assert event["payload"]["traffic_condition"] in {"low", "medium", "heavy"}


def test_vehicle_telemetry_event_uses_fleet_contract() -> None:
    """Verify the fleet application has an independent telemetry contract."""
    event = build_vehicle_telemetry_event(1, Random(42))
    assert event["schema_version"] == 1
    assert event["event_type"] == "VEHICLE_TELEMETRY"
    assert event["event_id"]
    assert event["event_timestamp"].endswith("Z")
    assert event["ingestion_timestamp"].endswith("Z")
    assert event["vehicle_id"].startswith("VEH-")
    assert event["driver_id"].startswith("DRV-")
    assert event["speed_kmh"] > 100
    assert 0 <= event["fuel_level_pct"] <= 100
    assert event["engine_status"] in {"RUNNING", "IDLE", "OFF"}
    assert event["vehicle_status"] in {"IN_TRANSIT", "IDLE", "MAINTENANCE", "OFFLINE"}


def test_vehicle_telemetry_reference_scenario_uses_one_vehicle_timeline() -> None:
    """Protect the documented fleet E2E scenario for windowed SQL metrics."""
    events = [build_vehicle_telemetry_event(index, Random(42 + index)) for index in range(4)]
    assert [event["vehicle_id"] for event in events] == ["VEH-101"] * 4
    assert [event["speed_kmh"] for event in events] == [62.0, 108.0, 75.0, 70.0]
    assert [event["fuel_level_pct"] for event in events] == [68.0, 66.0, 14.0, 13.0]
    assert [event["engine_temperature_c"] for event in events] == [88.0, 91.0, 94.0, 108.0]

    event_times = [
        datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
        for event in events
    ]
    assert [(event_times[index] - event_times[0]).total_seconds() for index in range(4)] == [0, 180, 360, 540]
