from __future__ import annotations

"""Daily batch KPI job for DeliveryFlow.

This Spark application exists to bridge the streaming serving layer and the
analytical lakehouse path. It reads operational delivery events from ClickHouse,
writes raw and gold Iceberg tables through Nessie, then publishes dashboard-ready
daily KPI rows back to ClickHouse for Superset.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone

import clickhouse_connect
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse connection settings shared by Spark JDBC and Python inserts."""

    host: str
    http_port: int
    database: str
    user: str
    password: str
    jdbc_driver: str = "com.clickhouse.jdbc.ClickHouseDriver"

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Load local/container ClickHouse settings without hardcoding runtime env."""
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            http_port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DATABASE", "delivery"),
            user=os.getenv("CLICKHOUSE_USER", "delivery_app"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password"),
        )

    @property
    def jdbc_url(self) -> str:
        """Build the Spark JDBC URL used when reading ClickHouse events."""
        return f"jdbc:clickhouse://{self.host}:{self.http_port}/{self.database}"


@dataclass(frozen=True)
class DailyKpiJobConfig:
    """Table names and application identity for the daily KPI job."""

    app_name: str = "deliveryflow-daily-kpi"
    raw_table: str = "nessie.bronze.raw_delivery_events"
    gold_table: str = "nessie.gold.daily_delivery_kpi"
    clickhouse_source_table: str = "delivery.delivery_events"
    clickhouse_target_table: str = "daily_delivery_kpi"


class DeliveryEventReader:
    """Read event facts from ClickHouse into Spark.

    The reader isolates JDBC details from transformation logic so the KPI code
    can work with a normal Spark DataFrame.
    """

    def __init__(self, spark: SparkSession, clickhouse: ClickHouseConfig, source_table: str) -> None:
        self.spark = spark
        self.clickhouse = clickhouse
        self.source_table = source_table

    def read(self) -> DataFrame:
        """Load the selected ClickHouse event fields required for KPI output."""
        return (
            self.spark.read.format("jdbc")
            .option("url", self.clickhouse.jdbc_url)
            .option("dbtable", self._source_query())
            .option("user", self.clickhouse.user)
            .option("password", self.clickhouse.password)
            .option("driver", self.clickhouse.jdbc_driver)
            .load()
        )

    def _source_query(self) -> str:
        """Return a JDBC subquery that normalizes ClickHouse timestamp output."""
        return f"""
            (
                SELECT
                    schema_version,
                    event_id,
                    event_type,
                    toString(event_timestamp) AS event_timestamp,
                    delivery_id,
                    shipment_id,
                    order_id,
                    vehicle_id,
                    driver_id,
                    route_id,
                    warehouse_id,
                    region,
                    status,
                    latitude,
                    longitude,
                    delay_minutes,
                    capacity_used,
                    capacity_total,
                    service_level,
                    priority,
                    customer_id,
                    destination_city,
                    package_count,
                    order_value,
                    payment_method,
                    planned_distance_km,
                    traffic_condition,
                    weather_condition
                FROM {self.source_table}
            ) AS delivery_events
            """


class DeliveryEventTransformer:
    """Derive analytical columns and aggregate daily delivery KPIs."""

    def enrich(self, events: DataFrame) -> DataFrame:
        """Add reusable business flags used by both raw and gold outputs."""
        return (
            events.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
            .withColumn("business_date", F.to_date("event_timestamp"))
            .withColumn("is_completed", F.when(F.col("status") == F.lit("DELIVERED"), F.lit(1)).otherwise(F.lit(0)))
            .withColumn("is_delayed", F.when(F.col("delay_minutes") > F.lit(0), F.lit(1)).otherwise(F.lit(0)))
            .withColumn(
                "utilization_ratio",
                F.when(F.col("capacity_total") > F.lit(0), F.col("capacity_used") / F.col("capacity_total")).otherwise(F.lit(0.0)),
            )
        )

    def build_daily_kpi(self, enriched: DataFrame) -> DataFrame:
        """Aggregate delivery performance by business date, region, and warehouse."""
        denominator = F.greatest(F.lit(1), F.col("completed_deliveries") + F.col("delayed_deliveries"))
        return (
            enriched.groupBy("business_date", "region", "warehouse_id")
            .agg(
                F.sum("is_completed").cast("long").alias("completed_deliveries"),
                F.sum("is_delayed").cast("long").alias("delayed_deliveries"),
                F.sum(F.when(F.col("is_delayed") == 0, 1).otherwise(0)).cast("long").alias("on_time_deliveries"),
                F.avg("delay_minutes").alias("avg_delay_minutes"),
                F.avg("utilization_ratio").alias("avg_vehicle_utilization"),
                F.sum("order_value").alias("total_order_value"),
                F.sum("package_count").cast("long").alias("total_package_count"),
                F.avg("planned_distance_km").alias("avg_distance_km"),
            )
            .withColumn("delay_rate", F.col("delayed_deliveries") / denominator)
            .withColumn("on_time_rate", F.col("on_time_deliveries") / denominator)
            .withColumn("published_at", F.lit(datetime.now(timezone.utc)))
            .withColumn("version", F.unix_timestamp(F.col("published_at")).cast("long"))
        )


class IcebergDailyKpiRepository:
    """Persist raw and gold KPI data into Nessie-managed Iceberg tables."""

    def __init__(self, spark: SparkSession, raw_table: str, gold_table: str) -> None:
        self.spark = spark
        self.raw_table = raw_table
        self.gold_table = gold_table

    def prepare_namespaces(self) -> None:
        """Create Iceberg namespaces so the job is repeatable on fresh volumes."""
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    def write_raw_events(self, enriched: DataFrame) -> None:
        """Write deduplicated raw events as the bronze analytical table."""
        (
            enriched.select(
                "schema_version",
                "event_id",
                "event_type",
                "event_timestamp",
                "delivery_id",
                "shipment_id",
                "order_id",
                "vehicle_id",
                "driver_id",
                "route_id",
                "warehouse_id",
                "region",
                "status",
                "latitude",
                "longitude",
                "delay_minutes",
                "capacity_used",
                "capacity_total",
                "service_level",
                "priority",
                "customer_id",
                "destination_city",
                "package_count",
                "order_value",
                "payment_method",
                "planned_distance_km",
                "traffic_condition",
                "weather_condition",
                "business_date",
            )
            .dropDuplicates(["event_id"])
            .writeTo(self.raw_table)
            .using("iceberg")
            .partitionedBy(F.col("business_date"))
            .createOrReplace()
        )

    def write_gold_kpi(self, kpi: DataFrame) -> None:
        """Write aggregated daily KPI rows as the gold analytical table."""
        kpi.writeTo(self.gold_table).using("iceberg").partitionedBy(F.col("business_date")).createOrReplace()


class ClickHouseDailyKpiPublisher:
    """Publish Spark KPI results back into ClickHouse for Superset serving."""

    columns = [
        "business_date",
        "region",
        "warehouse_id",
        "completed_deliveries",
        "delayed_deliveries",
        "on_time_deliveries",
        "delay_rate",
        "on_time_rate",
        "avg_delay_minutes",
        "avg_vehicle_utilization",
        "total_order_value",
        "total_package_count",
        "avg_distance_km",
        "published_at",
        "version",
    ]

    def __init__(self, clickhouse: ClickHouseConfig, target_table: str) -> None:
        self.clickhouse = clickhouse
        self.target_table = target_table

    def publish(self, kpi: DataFrame) -> None:
        """Collect the small KPI result set and insert it into ClickHouse."""
        rows = kpi.select(*self.columns).collect()
        client = clickhouse_connect.get_client(
            host=self.clickhouse.host,
            port=self.clickhouse.http_port,
            username=self.clickhouse.user,
            password=self.clickhouse.password,
            database=self.clickhouse.database,
        )
        try:
            client.insert(self.target_table, [tuple(row) for row in rows], column_names=self.columns)
        finally:
            client.close()


class DailyKpiJob:
    """Coordinate the end-to-end daily KPI batch workflow."""

    def __init__(self, spark: SparkSession, config: DailyKpiJobConfig, clickhouse: ClickHouseConfig) -> None:
        self.spark = spark
        self.config = config
        self.reader = DeliveryEventReader(spark, clickhouse, config.clickhouse_source_table)
        self.transformer = DeliveryEventTransformer()
        self.iceberg = IcebergDailyKpiRepository(spark, config.raw_table, config.gold_table)
        self.publisher = ClickHouseDailyKpiPublisher(clickhouse, config.clickhouse_target_table)

    @classmethod
    def create(cls) -> "DailyKpiJob":
        """Construct the job from default config and environment variables."""
        config = DailyKpiJobConfig()
        spark = SparkSession.builder.appName(config.app_name).getOrCreate()
        return cls(spark=spark, config=config, clickhouse=ClickHouseConfig.from_env())

    def run(self) -> None:
        """Run read, transform, Iceberg persistence, and ClickHouse publication."""
        events = self.reader.read()
        if events.rdd.isEmpty():
            raise RuntimeError("No delivery events found in ClickHouse for KPI calculation")

        enriched = self.transformer.enrich(events)
        kpi = self.transformer.build_daily_kpi(enriched)

        self.iceberg.prepare_namespaces()
        self.iceberg.write_raw_events(enriched)
        self.iceberg.write_gold_kpi(kpi)
        self.publisher.publish(kpi)

        print("DAILY_KPI_JOB_OK")

    def close(self) -> None:
        """Stop Spark so containerized runs release executor resources cleanly."""
        self.spark.stop()


def main() -> None:
    """Script entrypoint used by `spark-submit` and Airflow."""
    job = DailyKpiJob.create()
    try:
        job.run()
    finally:
        job.close()


if __name__ == "__main__":
    main()
