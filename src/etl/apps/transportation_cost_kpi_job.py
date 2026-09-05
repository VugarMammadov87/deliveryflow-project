from __future__ import annotations

"""Transportation cost batch KPI job for DeliveryFlow.

The job reads route-level cost facts from PostgreSQL, validates the batch
source, writes Bronze/Silver/Gold Iceberg tables, and publishes the Gold
serving aggregate to ClickHouse for Superset.
"""

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from deliveryflow_config import load_platform_config
from etl.framework.metadata import DatasetMetadata
from etl.framework.publishing import ClickHousePublisher
from etl.framework.quality import QualityValidator
from etl.framework.readers import PostgresReader
from etl.framework.writers import IcebergLayerWriter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


DEFAULT_METADATA_PATH = "/opt/deliveryflow/configs/datasets/delivery/transportation_costs.yaml"

CLICKHOUSE_GOLD_COLUMNS = [
    "business_date",
    "warehouse_id",
    "region",
    "route_id",
    "cost_record_count",
    "total_deliveries",
    "planned_cost",
    "actual_cost",
    "fuel_cost",
    "driver_cost",
    "toll_cost",
    "maintenance_cost",
    "other_cost",
    "total_distance_km",
    "avg_vehicle_utilization_pct",
    "avg_delivery_duration_minutes",
    "avg_duration_variance_minutes",
    "cost_variance",
    "cost_variance_pct",
    "cost_per_km",
    "cost_per_delivery",
    "fuel_cost_pct",
    "published_at",
    "version",
]


@dataclass(frozen=True)
class TransportationCostJobConfig:
    """Runtime settings for the transportation cost lakehouse batch job."""

    app_name: str
    metadata_path: Path
    pipeline_run_id: str
    batch_date: str | None
    source_system: str = "postgres.logistics_source.transportation_costs"

    @classmethod
    def from_env(cls) -> "TransportationCostJobConfig":
        """Build job settings from environment variables."""
        metadata_path = Path(os.getenv("TRANSPORTATION_COST_METADATA_PATH", DEFAULT_METADATA_PATH))
        return cls(
            app_name=os.getenv("TRANSPORTATION_COST_SPARK_APP", "deliveryflow-transportation-cost-kpi"),
            metadata_path=metadata_path,
            pipeline_run_id=os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4())),
            batch_date=os.getenv("BATCH_DATE"),
        )

    @property
    def batch_date_value(self) -> date | None:
        """Return the optional batch date as a Python date."""
        if not self.batch_date:
            return None
        return date.fromisoformat(self.batch_date)


class TransportationCostTransformer:
    """Build Bronze, Silver, and Gold transportation cost DataFrames."""

    def __init__(self, config: TransportationCostJobConfig) -> None:
        self.config = config

    def bronze(self, source: "DataFrame") -> "DataFrame":
        """Keep source-like records and add replay/audit metadata."""
        from pyspark.sql import functions as F

        batch_date = self.config.batch_date_value
        batch_date_expr = F.lit(batch_date.isoformat()).cast("date") if batch_date else F.col("business_date").cast("date")
        return (
            source.withColumn("business_date", F.col("business_date").cast("date"))
            .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)))
            .withColumn("_pipeline_run_id", F.lit(self.config.pipeline_run_id))
            .withColumn("_source_system", F.lit(self.config.source_system))
            .withColumn("_batch_date", batch_date_expr)
        )

    def silver(self, bronze: "DataFrame") -> "DataFrame":
        """Clean cost source records and calculate row-level business metrics."""
        from pyspark.sql import functions as F

        actual_total_cost = F.col("actual_total_cost").cast("double")
        planned_cost = F.col("planned_cost").cast("double")
        actual_distance_km = F.col("actual_distance_km").cast("double")
        planned_distance_km = F.col("planned_distance_km").cast("double")
        actual_duration_minutes = F.col("actual_duration_minutes").cast("double")
        planned_duration_minutes = F.col("planned_duration_minutes").cast("double")
        delivery_count = F.col("delivery_count").cast("double")
        vehicle_capacity = F.col("vehicle_capacity").cast("double")

        return (
            bronze.withColumn("cost_variance", actual_total_cost - planned_cost)
            .withColumn("cost_variance_pct", self._safe_ratio(actual_total_cost - planned_cost, planned_cost))
            .withColumn("distance_variance_km", actual_distance_km - planned_distance_km)
            .withColumn("distance_variance_pct", self._safe_ratio(actual_distance_km - planned_distance_km, planned_distance_km))
            .withColumn("duration_variance_minutes", actual_duration_minutes - planned_duration_minutes)
            .withColumn("cost_per_km", self._safe_ratio(actual_total_cost, actual_distance_km))
            .withColumn("cost_per_delivery", self._safe_ratio(actual_total_cost, delivery_count))
            .withColumn("fuel_cost_pct", self._safe_ratio(F.col("fuel_cost").cast("double"), actual_total_cost))
            .withColumn("vehicle_utilization_pct", self._safe_ratio(F.col("used_capacity").cast("double"), vehicle_capacity))
        )

    def gold(self, silver: "DataFrame") -> "DataFrame":
        """Aggregate BI-ready route performance metrics by date, warehouse, and route."""
        from pyspark.sql import functions as F

        grouped = silver.groupBy("business_date", "warehouse_id", "region", "route_id").agg(
            F.countDistinct("cost_id").cast("long").alias("cost_record_count"),
            F.sum("delivery_count").cast("long").alias("total_deliveries"),
            F.sum("planned_cost").alias("planned_cost"),
            F.sum("actual_total_cost").alias("actual_cost"),
            F.sum("fuel_cost").alias("fuel_cost"),
            F.sum("driver_cost").alias("driver_cost"),
            F.sum("toll_cost").alias("toll_cost"),
            F.sum("maintenance_cost").alias("maintenance_cost"),
            F.sum("other_cost").alias("other_cost"),
            F.sum("actual_distance_km").alias("total_distance_km"),
            F.avg("vehicle_utilization_pct").alias("avg_vehicle_utilization_pct"),
            F.avg("actual_duration_minutes").alias("avg_delivery_duration_minutes"),
            F.avg("duration_variance_minutes").alias("avg_duration_variance_minutes"),
        )
        actual_cost = F.col("actual_cost")
        planned_cost = F.col("planned_cost")
        total_distance_km = F.col("total_distance_km")
        total_deliveries = F.col("total_deliveries").cast("double")
        return (
            grouped.withColumn("cost_variance", actual_cost - planned_cost)
            .withColumn("cost_variance_pct", self._safe_ratio(actual_cost - planned_cost, planned_cost))
            .withColumn("cost_per_km", self._safe_ratio(actual_cost, total_distance_km))
            .withColumn("cost_per_delivery", self._safe_ratio(actual_cost, total_deliveries))
            .withColumn("fuel_cost_pct", self._safe_ratio(F.col("fuel_cost"), actual_cost))
            .withColumn("published_at", F.lit(datetime.now(timezone.utc)))
            .withColumn("version", F.unix_timestamp(F.col("published_at")).cast("long"))
        )

    @staticmethod
    def _safe_ratio(numerator: object, denominator: object) -> object:
        """Return numerator / denominator, yielding 0.0 for null or zero denominators."""
        from pyspark.sql import functions as F

        return F.when(denominator.isNull() | (denominator == 0), F.lit(0.0)).otherwise(numerator / denominator)


class TransportationCostKpiJob:
    """Coordinate transportation cost validation, lakehouse writes, and serving publish."""

    def __init__(
        self,
        spark: "SparkSession",
        config: TransportationCostJobConfig,
        metadata: DatasetMetadata,
        reader: PostgresReader,
        writer: IcebergLayerWriter,
        publisher: ClickHousePublisher,
    ) -> None:
        self.spark = spark
        self.config = config
        self.metadata = metadata
        self.reader = reader
        self.writer = writer
        self.publisher = publisher
        self.transformer = TransportationCostTransformer(config)
        self.quality = QualityValidator(metadata)

    @classmethod
    def create(cls) -> "TransportationCostKpiJob":
        """Construct the job from environment and shared platform configuration."""
        from pyspark.sql import SparkSession

        config = TransportationCostJobConfig.from_env()
        metadata = DatasetMetadata.from_file(config.metadata_path)
        spark = SparkSession.builder.appName(config.app_name).getOrCreate()
        platform = load_platform_config()
        return cls(
            spark=spark,
            config=config,
            metadata=metadata,
            reader=PostgresReader(platform.postgres("source")),
            writer=IcebergLayerWriter(),
            publisher=ClickHousePublisher(platform.clickhouse(application=metadata.application)),
        )

    def run(self) -> None:
        """Run source read, DQ validation, lakehouse writes, and ClickHouse publication."""
        source = self.reader.read(self.spark, self.metadata)
        if self.config.batch_date_value:
            from pyspark.sql import functions as F

            source = source.filter(F.col("business_date") == F.lit(self.config.batch_date_value.isoformat()).cast("date"))

        if source.rdd.isEmpty():
            suffix = f" for BATCH_DATE={self.config.batch_date}" if self.config.batch_date else ""
            raise RuntimeError(f"No transportation cost source rows found{suffix}")

        self.quality.validate_or_raise(source)

        bronze = self.transformer.bronze(source)
        silver = self.transformer.silver(bronze)
        gold = self.transformer.gold(silver)

        targets = self.metadata.iceberg_targets
        self._write_iceberg_table(bronze, targets["bronze_table"], "business_date")
        self._write_iceberg_table(silver, targets["silver_table"], "business_date")
        self._write_iceberg_table(gold, targets["gold_table"], "business_date")
        self._publish_clickhouse(gold)

        print("TRANSPORTATION_COST_KPI_JOB_OK")

    def _write_iceberg_table(self, dataframe: "DataFrame", table_name: str, partition_column: str) -> None:
        """Prepare and write one Iceberg layer table."""
        self.writer.prepare_namespace(self.spark, table_name)
        mode = "overwrite_partitions" if self.config.batch_date_value else "create_or_replace"
        self.writer.write_table(
            dataframe,
            table_name,
            partition_column=partition_column,
            mode=mode,
            spark=self.spark,
        )

    def _publish_clickhouse(self, dataframe: "DataFrame") -> int:
        """Publish the Gold dataframe to the configured ClickHouse serving table."""
        target = self.metadata.clickhouse_target
        return self.publisher.publish(dataframe, target["table"], CLICKHOUSE_GOLD_COLUMNS)

    def close(self) -> None:
        """Stop Spark and release cluster resources."""
        self.spark.stop()


def main() -> None:
    """Script entrypoint used by spark-submit."""
    job = TransportationCostKpiJob.create()
    try:
        job.run()
    finally:
        job.close()


if __name__ == "__main__":
    main()
