from __future__ import annotations

"""Tests for reusable ETL framework primitives."""

from deliveryflow_config import ClickHouseConnection, PostgresConnection
from etl.framework.metadata import DatasetMetadata
from etl.framework.publishing import ClickHousePublisher
from etl.framework.quality import QualityValidator
from etl.framework.readers import PostgresReader
from etl.framework.writers import IcebergLayerWriter


def _transportation_metadata() -> DatasetMetadata:
    """Return a minimal metadata object for framework tests."""
    return DatasetMetadata.from_file("configs/datasets/delivery/transportation_costs.yaml")


def test_dataset_metadata_loads_transportation_cost_source() -> None:
    """Verify the transportation dataset file is a valid batch metadata record."""
    metadata = _transportation_metadata()

    assert metadata.dataset_id == "delivery.transportation_costs"
    assert metadata.application == "delivery"
    assert metadata.postgres_schema == "public"
    assert metadata.postgres_table == "transportation_costs"
    assert metadata.primary_key == ["cost_id"]
    assert metadata.iceberg_targets["bronze_table"] == "nessie.bronze.transportation_costs"
    assert metadata.clickhouse_target["view"] == "v_transportation_cost_performance"


def test_dataset_metadata_rejects_missing_required_sections() -> None:
    """Fail fast when dataset metadata is not complete enough for ETL use."""
    try:
        DatasetMetadata.from_dict({"dataset_id": "broken"})
    except ValueError as exc:
        assert "application" in str(exc)
        assert "source" in str(exc)
    else:
        raise AssertionError("DatasetMetadata accepted incomplete metadata")


def test_postgres_reader_builds_jdbc_options_from_metadata() -> None:
    """Keep PostgreSQL Spark read options centralized and metadata-backed."""
    metadata = _transportation_metadata()
    reader = PostgresReader(
        PostgresConnection(
            host="postgres-source",
            port=5432,
            database="logistics_source",
            user="source_user",
            password="source_password",
        )
    )

    options = reader.options_for(metadata)

    assert options["url"] == "jdbc:postgresql://postgres-source:5432/logistics_source"
    assert options["dbtable"] == "public.transportation_costs"
    assert options["user"] == "source_user"
    assert options["driver"] == "org.postgresql.Driver"


def test_quality_validator_detects_missing_columns() -> None:
    """Validate metadata column references before Spark actions execute."""
    validator = QualityValidator(_transportation_metadata())

    try:
        validator.validate_columns(["cost_id", "business_date"])
    except ValueError as exc:
        assert "actual_total_cost" in str(exc)
        assert "route_id" in str(exc)
    else:
        raise AssertionError("QualityValidator accepted an incomplete column set")


def test_iceberg_writer_extracts_namespace() -> None:
    """Verify namespace parsing for catalog-qualified Iceberg table names."""
    writer = IcebergLayerWriter()

    assert writer.namespace_for("nessie.silver.transportation_costs_enriched") == "nessie.silver"


def test_clickhouse_publisher_collects_selected_columns() -> None:
    """Verify publisher materializes only requested BI serving columns."""
    inserted: dict[str, object] = {}

    class FakeDataFrame:
        def select(self, *columns: str) -> "FakeDataFrame":
            inserted["selected"] = columns
            return self

        def collect(self) -> list[tuple[object, ...]]:
            return [("2026-09-04", "baku", 450.0)]

    class CapturingPublisher(ClickHousePublisher):
        def insert_rows(self, table_name: str, columns: list[str], rows: object) -> None:
            inserted.update({"table": table_name, "columns": columns, "rows": list(rows)})

    publisher = CapturingPublisher(
        ClickHouseConnection(
            host="clickhouse",
            http_port=8123,
            http_url="http://clickhouse:8123",
            database="delivery",
            user="delivery_app",
            password="local-clickhouse-password",
        )
    )

    count = publisher.publish(FakeDataFrame(), "daily_transportation_cost_kpi", ["business_date", "region", "actual_cost"])

    assert count == 1
    assert inserted["selected"] == ("business_date", "region", "actual_cost")
    assert inserted["table"] == "daily_transportation_cost_kpi"
    assert inserted["rows"] == [("2026-09-04", "baku", 450.0)]


def test_iceberg_writer_supports_partition_overwrite() -> None:
    """Verify write_table calls overwritePartitions when partition mode is selected."""
    actions: list[str] = []

    class FakeWriter:
        def partitionedBy(self, col: object) -> "FakeWriter":
            actions.append("partitioned")
            return self

        def overwritePartitions(self) -> None:
            actions.append("overwrite_partitions")

        def createOrReplace(self) -> None:
            actions.append("create_or_replace")

    class FakeDataFrame:
        def writeTo(self, table_name: str) -> "FakeDataFrame":
            return self

        def using(self, format_name: str) -> FakeWriter:
            actions.append(f"using_{format_name}")
            return FakeWriter()

    class FakeCatalog:
        def tableExists(self, name: str) -> bool:
            return True

    class FakeSpark:
        catalog = FakeCatalog()

    writer = IcebergLayerWriter()
    writer.write_table(
        FakeDataFrame(),
        "nessie.silver.test_table",
        partition_column="business_date",
        mode="overwrite_partitions",
        spark=FakeSpark(),
    )

    assert actions == ["using_iceberg", "partitioned", "overwrite_partitions"]
