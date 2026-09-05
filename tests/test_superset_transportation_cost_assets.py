from __future__ import annotations

"""Tests for Transportation Cost Superset BI-as-Code assets."""

from pathlib import Path
import importlib.util
import sys

import yaml


ASSET_FILE = Path("configs/superset/deliveryflow_bi.yaml")
IMPORTER_FILE = Path("scripts/import_superset_assets.py")

spec = importlib.util.spec_from_file_location("import_superset_assets", IMPORTER_FILE)
assert spec is not None
importer = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = importer
spec.loader.exec_module(importer)


def load_assets() -> dict[str, object]:
    """Load the source-controlled Superset BI asset spec."""
    return yaml.safe_load(ASSET_FILE.read_text(encoding="utf-8"))


def test_transportation_cost_dataset_is_declared() -> None:
    """Verify Dashboard 6 reads from the ClickHouse serving view."""
    assets = load_assets()
    datasets = {dataset["name"]: dataset for dataset in assets["datasets"]}

    dataset = datasets["Transportation Cost Performance"]

    assert dataset["schema"] == "delivery"
    assert dataset["table_name"] == "v_transportation_cost_performance"
    assert dataset["main_dttm_col"] == "business_date"
    assert dataset["temporal_columns"] == ["business_date"]


def test_transportation_cost_dashboard_uses_practical_management_charts() -> None:
    """Verify Dashboard 6 includes KPI, cost, route, warehouse, and delay analysis."""
    assets = load_assets()
    dashboards = {dashboard["title"]: dashboard for dashboard in importer.dashboard_specs(assets)}
    charts = {chart["name"]: chart for chart in assets["charts"]}

    dashboard = dashboards["Transportation Cost & Route Performance"]

    assert dashboard["slug"] == "transportation-cost-route-performance"
    assert dashboard["charts"] == [
        "Actual Transportation Cost",
        "Transportation Cost Variance",
        "Transportation Cost per Delivery",
        "Transportation Cost Breakdown",
        "Route Cost Performance",
        "Warehouse Region Transportation Cost",
        "Route Cost vs Delay Risk",
    ]
    for chart_name in dashboard["charts"]:
        assert charts[chart_name]["dataset"] == "Transportation Cost Performance"


def test_transportation_cost_charts_cover_required_metrics() -> None:
    """Verify the BI assets expose the core transportation cost business questions."""
    assets = load_assets()
    charts = {chart["name"]: chart for chart in assets["charts"]}

    assert charts["Actual Transportation Cost"]["params"]["metric"] == "actual_cost"
    assert charts["Transportation Cost Variance"]["params"]["metric"] == "cost_variance"
    assert charts["Transportation Cost per Delivery"]["params"]["metric"] == "cost_per_delivery"
    assert charts["Transportation Cost Breakdown"]["params"]["metrics"] == [
        "fuel_cost",
        "driver_cost",
        "toll_cost",
        "maintenance_cost",
        "other_cost",
    ]
    assert "delay_rate" in charts["Route Cost vs Delay Risk"]["params"]["metrics"]
    assert "cost_delay_risk_score" in charts["Route Cost vs Delay Risk"]["params"]["metrics"]


def test_importer_supports_multiple_dashboards_without_breaking_legacy_dashboard() -> None:
    """Verify the importer can load the existing dashboard and Dashboard 6."""
    assets = load_assets()
    dashboards = importer.dashboard_specs(assets)

    assert [dashboard["title"] for dashboard in dashboards] == [
        "DeliveryFlow Operations Dashboard",
        "Transportation Cost & Route Performance",
    ]


def test_importer_normalizes_single_big_number_metric() -> None:
    """Verify readable big-number metric specs become Superset adhoc metrics."""
    normalized = importer.normalize_chart_params(
        {"metric": "actual_cost"},
        {"columns": [{"column_name": "actual_cost", "type": "Float64", "type_generic": 0}]},
    )

    metric = normalized["metric"]

    assert metric["expressionType"] == "SIMPLE"
    assert metric["aggregate"] == "SUM"
    assert metric["label"] == "actual_cost"


def test_transportation_cost_dashboard_declares_native_filters() -> None:
    """Verify Dashboard 6 includes business date, warehouse, region, and route filters."""
    assets = load_assets()
    dashboards = {dashboard["title"]: dashboard for dashboard in importer.dashboard_specs(assets)}
    dashboard = dashboards["Transportation Cost & Route Performance"]

    filters = dashboard.get("filters", [])
    filter_cols = [f["column"] for f in filters]

    assert "business_date" in filter_cols
    assert "warehouse_id" in filter_cols
    assert "region" in filter_cols
    assert "route_id" in filter_cols

    native_cfg = importer.build_native_filter_configuration(filters)
    assert len(native_cfg) == 4
    assert native_cfg[0]["filterType"] == "filter_time"
    assert native_cfg[1]["filterType"] == "filter_select"
