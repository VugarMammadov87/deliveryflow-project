from __future__ import annotations

"""Tests for the transportation cost Spark batch application orchestration."""

from pathlib import Path

from etl.apps.transportation_cost_kpi_job import (
    CLICKHOUSE_GOLD_COLUMNS,
    TransportationCostJobConfig,
    TransportationCostKpiJob,
)
from etl.framework.metadata import DatasetMetadata


class FakeRdd:
    """Small RDD stand-in for source emptiness checks."""

    def __init__(self, empty: bool = False) -> None:
        self.empty = empty

    def isEmpty(self) -> bool:
        """Return whether the fake DataFrame is empty."""
        return self.empty


class FakeDataFrame:
    """Small DataFrame stand-in for orchestration tests."""

    columns = [
        "cost_id",
        "business_date",
        "delivery_id",
        "route_id",
        "vehicle_id",
        "warehouse_id",
        "region",
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

    def __init__(self, name: str = "source") -> None:
        self.name = name
        self.rdd = FakeRdd()


class FakeReader:
    """Reader stand-in that records metadata use."""

    def __init__(self) -> None:
        self.metadata_id = ""

    def read(self, spark: object, metadata: DatasetMetadata) -> FakeDataFrame:
        """Return a fake source DataFrame."""
        self.metadata_id = metadata.dataset_id
        return FakeDataFrame()


class FakeWriter:
    """Writer stand-in that records Iceberg writes."""

    def __init__(self) -> None:
        self.namespaces: list[str] = []
        self.writes: list[tuple[str, str]] = []

    def prepare_namespace(self, spark: object, table_name: str) -> None:
        """Record namespace preparation."""
        self.namespaces.append(table_name)

    def write_table(
        self,
        dataframe: FakeDataFrame,
        table_name: str,
        *,
        partition_column: str | None = None,
        mode: str = "create_or_replace",
        spark: object | None = None,
        **kwargs: object,
    ) -> None:
        """Record table writes."""
        self.writes.append((dataframe.name, table_name))


class FakePublisher:
    """Publisher stand-in that records ClickHouse publication."""

    def __init__(self) -> None:
        self.published: list[tuple[str, list[str], str]] = []

    def publish(self, dataframe: FakeDataFrame, table_name: str, columns: list[str]) -> int:
        """Record publish target and selected columns."""
        self.published.append((table_name, columns, dataframe.name))
        return 1


class FakeTransformer:
    """Transformer stand-in for testing job orchestration without PySpark."""

    def bronze(self, source: FakeDataFrame) -> FakeDataFrame:
        """Return the fake Bronze DataFrame."""
        return FakeDataFrame("bronze")

    def silver(self, bronze: FakeDataFrame) -> FakeDataFrame:
        """Return the fake Silver DataFrame."""
        return FakeDataFrame("silver")

    def gold(self, silver: FakeDataFrame) -> FakeDataFrame:
        """Return the fake Gold DataFrame."""
        return FakeDataFrame("gold")


class FakeQuality:
    """Quality validator stand-in."""

    def __init__(self) -> None:
        self.validated = False

    def validate_or_raise(self, dataframe: FakeDataFrame) -> list[object]:
        """Record quality validation."""
        self.validated = True
        return []


def test_transportation_cost_config_reads_environment(monkeypatch) -> None:
    """Verify job config can be controlled by scheduler/runtime environment."""
    monkeypatch.setenv("TRANSPORTATION_COST_METADATA_PATH", "configs/datasets/delivery/transportation_costs.yaml")
    monkeypatch.setenv("TRANSPORTATION_COST_SPARK_APP", "test-app")
    monkeypatch.setenv("PIPELINE_RUN_ID", "run-123")
    monkeypatch.setenv("BATCH_DATE", "2026-09-04")

    config = TransportationCostJobConfig.from_env()

    assert config.metadata_path == Path("configs/datasets/delivery/transportation_costs.yaml")
    assert config.app_name == "test-app"
    assert config.pipeline_run_id == "run-123"
    assert config.batch_date_value.isoformat() == "2026-09-04"


def test_transportation_cost_job_writes_bronze_silver_gold(monkeypatch) -> None:
    """Verify the app orchestrates quality and all three lakehouse layers."""
    metadata = DatasetMetadata.from_file("configs/datasets/delivery/transportation_costs.yaml")
    reader = FakeReader()
    writer = FakeWriter()
    publisher = FakePublisher()
    config = TransportationCostJobConfig(
        app_name="test-app",
        metadata_path=Path("configs/datasets/delivery/transportation_costs.yaml"),
        pipeline_run_id="run-123",
        batch_date=None,
    )
    job = TransportationCostKpiJob(
        spark=object(),
        config=config,
        metadata=metadata,
        reader=reader,
        writer=writer,
        publisher=publisher,
    )
    quality = FakeQuality()
    job.transformer = FakeTransformer()
    job.quality = quality

    job.run()

    assert reader.metadata_id == "delivery.transportation_costs"
    assert quality.validated is True
    assert writer.writes == [
        ("bronze", "nessie.bronze.transportation_costs"),
        ("silver", "nessie.silver.transportation_costs_enriched"),
        ("gold", "nessie.gold.daily_transportation_cost_kpi"),
    ]
    assert publisher.published == [
        ("daily_transportation_cost_kpi", CLICKHOUSE_GOLD_COLUMNS, "gold"),
    ]
