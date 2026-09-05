from __future__ import annotations

"""Tests for synthetic transportation cost batch source records."""

from decimal import Decimal
from random import Random

from producers.events import build_event
from producers.transportation_costs import build_transportation_cost


def _money(value: object) -> Decimal:
    """Normalize generated money values for exact two-decimal assertions."""
    return Decimal(str(value)).quantize(Decimal("0.01"))


def test_transportation_cost_reuses_delivery_identifiers() -> None:
    """Keep the batch cost source joinable to existing delivery source rows."""
    event = build_event(7, Random(42))
    cost = build_transportation_cost(7, Random(42))

    assert cost["delivery_id"] == event["delivery_id"]
    assert cost["route_id"] == event["route_id"]
    assert cost["vehicle_id"] == event["vehicle_id"]
    assert cost["warehouse_id"] == event["warehouse_id"]
    assert cost["region"] == event["payload"]["region"]


def test_transportation_cost_components_match_total() -> None:
    """Protect the source-system accounting rule used by downstream ETL."""
    cost = build_transportation_cost(3, Random(42))

    component_total = sum(
        _money(cost[column])
        for column in ["fuel_cost", "driver_cost", "toll_cost", "maintenance_cost", "other_cost"]
    )

    assert _money(cost["actual_total_cost"]) == component_total


def test_transportation_cost_numeric_fields_are_valid() -> None:
    """Verify generated source rows satisfy PostgreSQL quality constraints."""
    cost = build_transportation_cost(11, Random(42))
    non_negative_columns = [
        "planned_distance_km",
        "actual_distance_km",
        "planned_cost",
        "fuel_cost",
        "driver_cost",
        "toll_cost",
        "maintenance_cost",
        "other_cost",
        "actual_total_cost",
        "fuel_liters",
        "delivery_count",
        "used_capacity",
        "planned_duration_minutes",
        "actual_duration_minutes",
    ]

    assert cost["cost_id"]
    assert cost["business_date"]
    assert all(cost[column] >= 0 for column in non_negative_columns)
    assert cost["vehicle_capacity"] > 0
    assert cost["used_capacity"] <= cost["vehicle_capacity"]
