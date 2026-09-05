from __future__ import annotations

"""Static checks for the transportation cost ClickHouse serving schema."""

from pathlib import Path


SCHEMA_SQL = Path("services/clickhouse/init/001_delivery_schema.sql")


def test_transportation_cost_serving_table_is_declared() -> None:
    """Verify the route-level transportation cost KPI table is versioned."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS delivery.daily_transportation_cost_kpi" in sql
    assert "cost_record_count UInt64" in sql
    assert "total_deliveries UInt64" in sql
    assert "actual_cost Float64" in sql
    assert "cost_per_delivery Float64" in sql
    assert "fuel_cost_pct Float64" in sql
    assert "ENGINE = ReplacingMergeTree(version)" in sql
    assert "ORDER BY (business_date, warehouse_id, region, route_id)" in sql


def test_transportation_cost_bi_view_combines_cost_and_delay() -> None:
    """Keep Dashboard 6 sourced from a BI-ready ClickHouse view."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")

    assert "CREATE VIEW IF NOT EXISTS delivery.v_transportation_cost_performance" in sql
    assert "FROM delivery.daily_transportation_cost_kpi FINAL AS k" in sql
    assert "FROM delivery.delivery_events" in sql
    assert "delayed_delivery_count" in sql
    assert "avg_delay_minutes" in sql
    assert "cost_delay_risk_score" in sql
