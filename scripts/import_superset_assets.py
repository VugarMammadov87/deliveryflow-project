from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml


@dataclass(frozen=True)
class SupersetConfig:
    base_url: str
    username: str
    password: str
    asset_file: Path
    timeout_seconds: int

    @classmethod
    def from_env(cls) -> "SupersetConfig":
        return cls(
            base_url=os.getenv("SUPERSET_BASE_URL", "http://superset:8088").rstrip("/"),
            username=os.getenv("SUPERSET_ADMIN_USER", "admin"),
            password=os.getenv("SUPERSET_ADMIN_PASSWORD", "admin"),
            asset_file=Path(os.getenv("SUPERSET_ASSET_FILE", "/app/configs/superset/deliveryflow_bi.yaml")),
            timeout_seconds=int(os.getenv("SUPERSET_IMPORT_TIMEOUT_SECONDS", "120")),
        )


class SupersetClient:
    def __init__(self, config: SupersetConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.csrf_token = ""

    def wait_until_ready(self) -> None:
        deadline = time.time() + self.config.timeout_seconds
        while time.time() < deadline:
            try:
                response = self.session.get(f"{self.config.base_url}/health", timeout=5)
                if response.ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        raise RuntimeError(f"Superset did not become ready within {self.config.timeout_seconds}s")

    def login(self) -> None:
        response = self.session.post(
            f"{self.config.base_url}/api/v1/security/login",
            json={
                "username": self.config.username,
                "password": self.config.password,
                "provider": "db",
                "refresh": True,
            },
            timeout=10,
        )
        response.raise_for_status()
        access_token = response.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

        csrf = self.session.get(f"{self.config.base_url}/api/v1/security/csrf_token/", timeout=10)
        csrf.raise_for_status()
        self.csrf_token = csrf.json()["result"]
        self.session.headers.update({"X-CSRFToken": self.csrf_token})

    def get_all(self, path: str) -> list[dict[str, Any]]:
        response = self.session.get(f"{self.config.base_url}{path}", params={"page_size": 1000}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result", [])
        if isinstance(result, dict):
            return result.get("data", [])
        return result

    def create(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.post(f"{self.config.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return normalize_item_response(response.json())

    def update(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.put(f"{self.config.base_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return normalize_item_response(response.json())


def normalize_item_response(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("result")
    if isinstance(result, dict):
        item = dict(result)
    else:
        item = {}
    if "id" in payload and "id" not in item:
        item["id"] = payload["id"]
    return item


def find_by_name(items: list[dict[str, Any]], name_fields: tuple[str, ...], value: str) -> dict[str, Any] | None:
    for item in items:
        for field in name_fields:
            if item.get(field) == value:
                return item
    return None


def database_id(client: SupersetClient, spec: dict[str, Any]) -> int:
    existing = find_by_name(client.get_all("/api/v1/database/"), ("database_name", "database_name_text"), spec["name"])
    payload = {
        "database_name": spec["name"],
        "sqlalchemy_uri": spec["sqlalchemy_uri"],
        "expose_in_sqllab": bool(spec.get("expose_in_sqllab", True)),
    }
    if existing:
        db_id = int(existing["id"])
        client.update(f"/api/v1/database/{db_id}", payload)
        return db_id
    return int(client.create("/api/v1/database/", payload)["id"])


def dataset_id(client: SupersetClient, db_id: int, spec: dict[str, Any]) -> int:
    table_name = spec["table_name"]
    existing = None
    for dataset in client.get_all("/api/v1/dataset/"):
        if dataset.get("table_name") == table_name and dataset.get("schema") == spec.get("schema"):
            existing = dataset
            break

    payload = {
        "database": db_id,
        "schema": spec.get("schema"),
        "table_name": table_name,
    }
    if existing:
        return int(existing["id"])
    return int(client.create("/api/v1/dataset/", payload)["id"])


def chart_id(client: SupersetClient, dataset_ids: dict[str, int], spec: dict[str, Any]) -> int:
    existing = find_by_name(client.get_all("/api/v1/chart/"), ("slice_name",), spec["name"])
    payload = {
        "slice_name": spec["name"],
        "viz_type": spec["viz_type"],
        "datasource_id": dataset_ids[spec["dataset"]],
        "datasource_type": "table",
        "params": json.dumps(spec.get("params", {}), separators=(",", ":")),
    }
    if existing:
        chart_pk = int(existing["id"])
        client.update(f"/api/v1/chart/{chart_pk}", payload)
        return chart_pk
    return int(client.create("/api/v1/chart/", payload)["id"])


def dashboard_id(client: SupersetClient, chart_ids: dict[str, int], spec: dict[str, Any]) -> int:
    existing = find_by_name(client.get_all("/api/v1/dashboard/"), ("dashboard_title",), spec["title"])
    metadata = {
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
    }
    payload = {
        "dashboard_title": spec["title"],
        "slug": spec["slug"],
        "published": bool(spec.get("published", True)),
        "json_metadata": json.dumps(metadata, separators=(",", ":")),
        "position_json": json.dumps(build_position_json(spec["charts"], chart_ids), separators=(",", ":")),
    }
    if existing:
        dashboard_pk = int(existing["id"])
        client.update(f"/api/v1/dashboard/{dashboard_pk}", payload)
        attach_charts_to_dashboard(client, dashboard_pk, chart_ids)
        return dashboard_pk
    dashboard_pk = int(client.create("/api/v1/dashboard/", payload)["id"])
    attach_charts_to_dashboard(client, dashboard_pk, chart_ids)
    return dashboard_pk


def attach_charts_to_dashboard(client: SupersetClient, dashboard_pk: int, chart_ids: dict[str, int]) -> None:
    for chart_pk in chart_ids.values():
        client.update(f"/api/v1/chart/{chart_pk}", {"dashboards": [dashboard_pk]})


def build_position_json(chart_names: list[str], chart_ids: dict[str, int]) -> dict[str, Any]:
    position: dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"], "meta": {}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "meta": {}},
    }
    for index, name in enumerate(chart_names):
        row_id = f"ROW-{index}"
        chart_id = f"CHART-{chart_ids[name]}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        position[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "meta": {"chartId": chart_ids[name], "height": 50, "width": 12, "sliceName": name},
        }
    return position


def main() -> int:
    config = SupersetConfig.from_env()
    asset_spec = yaml.safe_load(config.asset_file.read_text(encoding="utf-8"))
    client = SupersetClient(config)
    client.wait_until_ready()
    client.login()

    db_id = database_id(client, asset_spec["database"])
    dataset_ids = {spec["name"]: dataset_id(client, db_id, spec) for spec in asset_spec["datasets"]}
    chart_ids = {spec["name"]: chart_id(client, dataset_ids, spec) for spec in asset_spec["charts"]}
    dash_id = dashboard_id(client, chart_ids, asset_spec["dashboard"])

    print(f"SUPERSET_BI_AS_CODE_IMPORT_OK dashboard_id={dash_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
