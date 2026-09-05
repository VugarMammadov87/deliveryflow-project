"""Synthetic transportation cost source records for batch analytics.

The functions in this module create realistic transportation cost facts that
can be loaded into PostgreSQL before Spark builds lakehouse and BI layers.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from random import Random
from typing import Any


def build_transportation_cost(index: int, rng: Random) -> dict[str, Any]:
    """Build one route-level transportation cost source record.

    The generated row reuses DeliveryFlow delivery, route, vehicle, warehouse,
    and region identifiers so the future Spark job can combine cost with
    operational delivery performance.
    """
    regions = ["baku", "absheron", "ganja", "sumgait"]
    region = regions[index % len(regions)]
    route_number = index % 6
    vehicle_number = index % 8
    delivery_number = index % 24
    business_date = date.today() - timedelta(days=index % 7)

    route_profile = _route_profile(region, route_number)
    planned_distance_km = route_profile["planned_distance_km"]
    distance_variance_pct = route_profile["distance_variance_pct"] + rng.uniform(-0.04, 0.05)
    actual_distance_km = max(0.0, round(planned_distance_km * (1 + distance_variance_pct), 2))

    delivery_count = 28 + ((index * 7) % 64)
    vehicle_capacity = [80.0, 100.0, 120.0, 160.0][vehicle_number % 4]
    used_capacity = min(vehicle_capacity, round(vehicle_capacity * (0.45 + ((index % 7) * 0.07)), 2))
    fuel_liters = round(actual_distance_km * route_profile["fuel_liters_per_km"], 2)

    planned_cost = round(planned_distance_km * route_profile["planned_cost_per_km"] + delivery_count * 1.15, 2)
    fuel_cost = round(fuel_liters * route_profile["fuel_price_per_liter"], 2)
    driver_cost = round(route_profile["driver_base_cost"] + route_profile["driver_cost_per_minute"] * route_profile["planned_duration_minutes"], 2)
    toll_cost = round(route_profile["toll_cost"], 2)
    maintenance_cost = round(actual_distance_km * route_profile["maintenance_cost_per_km"], 2)
    other_cost = round(8.0 + ((index % 5) * 3.5) + rng.uniform(0, 4), 2)
    actual_total_cost = round(fuel_cost + driver_cost + toll_cost + maintenance_cost + other_cost, 2)

    planned_duration_minutes = route_profile["planned_duration_minutes"]
    duration_variance_minutes = route_profile["duration_variance_minutes"] + ((index % 5) * 4)
    actual_duration_minutes = max(0, planned_duration_minutes + duration_variance_minutes)
    now = datetime.now(UTC).replace(microsecond=0)

    return {
        "cost_id": f"transport-cost-{business_date.isoformat()}-{region}-{route_number:03d}-{vehicle_number:03d}",
        "business_date": business_date,
        "delivery_id": f"delivery-{delivery_number:04d}",
        "route_id": f"route-{region}-{route_number:03d}",
        "vehicle_id": f"vehicle-{vehicle_number:03d}",
        "warehouse_id": f"warehouse-{region}-01",
        "region": region,
        "planned_distance_km": planned_distance_km,
        "actual_distance_km": actual_distance_km,
        "planned_cost": planned_cost,
        "fuel_cost": fuel_cost,
        "driver_cost": driver_cost,
        "toll_cost": toll_cost,
        "maintenance_cost": maintenance_cost,
        "other_cost": other_cost,
        "actual_total_cost": actual_total_cost,
        "fuel_liters": fuel_liters,
        "delivery_count": delivery_count,
        "vehicle_capacity": vehicle_capacity,
        "used_capacity": used_capacity,
        "planned_duration_minutes": planned_duration_minutes,
        "actual_duration_minutes": actual_duration_minutes,
        "created_at": now,
        "updated_at": now,
    }


def _route_profile(region: str, route_number: int) -> dict[str, float | int]:
    """Return stable cost and duration assumptions for a route family."""
    regional_profiles = {
        "baku": {
            "planned_distance_km": 48.0,
            "planned_cost_per_km": 2.45,
            "fuel_liters_per_km": 0.24,
            "fuel_price_per_liter": 1.18,
            "driver_base_cost": 62.0,
            "driver_cost_per_minute": 0.42,
            "toll_cost": 6.0,
            "maintenance_cost_per_km": 0.21,
            "planned_duration_minutes": 95,
            "distance_variance_pct": 0.08,
            "duration_variance_minutes": 18,
        },
        "absheron": {
            "planned_distance_km": 82.0,
            "planned_cost_per_km": 2.1,
            "fuel_liters_per_km": 0.27,
            "fuel_price_per_liter": 1.18,
            "driver_base_cost": 70.0,
            "driver_cost_per_minute": 0.39,
            "toll_cost": 12.0,
            "maintenance_cost_per_km": 0.23,
            "planned_duration_minutes": 130,
            "distance_variance_pct": 0.04,
            "duration_variance_minutes": 12,
        },
        "ganja": {
            "planned_distance_km": 365.0,
            "planned_cost_per_km": 1.9,
            "fuel_liters_per_km": 0.31,
            "fuel_price_per_liter": 1.18,
            "driver_base_cost": 155.0,
            "driver_cost_per_minute": 0.35,
            "toll_cost": 28.0,
            "maintenance_cost_per_km": 0.29,
            "planned_duration_minutes": 360,
            "distance_variance_pct": -0.02,
            "duration_variance_minutes": 25,
        },
        "sumgait": {
            "planned_distance_km": 118.0,
            "planned_cost_per_km": 2.0,
            "fuel_liters_per_km": 0.28,
            "fuel_price_per_liter": 1.18,
            "driver_base_cost": 86.0,
            "driver_cost_per_minute": 0.38,
            "toll_cost": 18.0,
            "maintenance_cost_per_km": 0.25,
            "planned_duration_minutes": 150,
            "distance_variance_pct": 0.12,
            "duration_variance_minutes": 35,
        },
    }
    profile = dict(regional_profiles[region])
    profile["planned_distance_km"] = round(float(profile["planned_distance_km"]) + route_number * 7.5, 2)
    profile["planned_duration_minutes"] = int(profile["planned_duration_minutes"]) + route_number * 8
    profile["toll_cost"] = round(float(profile["toll_cost"]) + (route_number % 3) * 4.0, 2)
    return profile
