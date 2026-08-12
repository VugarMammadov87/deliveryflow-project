from __future__ import annotations

from producers.synthetic_logistics_producer import build_event
from random import Random


def test_synthetic_event_uses_versioned_json_contract() -> None:
    event = build_event(0, Random(42))
    assert event["schema_version"] == 1
    assert event["event_id"]
    assert event["event_type"] in {"ORDER_LOADED", "VEHICLE_DEPARTED", "IN_TRANSIT", "DELIVERY_DELAYED", "DELIVERED"}
    assert event["event_timestamp"].endswith("Z")
    assert isinstance(event["payload"], dict)
    assert event["payload"]["capacity_total"] > 0
