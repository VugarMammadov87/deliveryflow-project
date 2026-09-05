from __future__ import annotations

"""Reusable Iceberg write helpers for Spark batch jobs."""

from dataclasses import dataclass


@dataclass(frozen=True)
class IcebergLayerWriter:
    """Write Spark DataFrames into named Iceberg tables."""

    def namespace_for(self, table_name: str) -> str:
        """Return the catalog namespace portion from a full Iceberg table name."""
        parts = table_name.split(".")
        if len(parts) < 3:
            raise ValueError(f"Iceberg table must include catalog, namespace, and table: {table_name}")
        return ".".join(parts[:-1])

    def prepare_namespace(self, spark: object, table_name: str) -> None:
        """Create the target Iceberg namespace if it does not already exist."""
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace_for(table_name)}")

    def write_table(
        self,
        dataframe: object,
        table_name: str,
        *,
        partition_column: str | None = None,
        mode: str = "create_or_replace",
        spark: object | None = None,
    ) -> None:
        """Write a DataFrame to Iceberg using the requested table mode."""
        writer = dataframe.writeTo(table_name).using("iceberg")
        if partition_column:
            try:
                from pyspark.sql import functions as F

                writer = writer.partitionedBy(F.col(partition_column))
            except (ModuleNotFoundError, ImportError):
                writer = writer.partitionedBy(partition_column)

        if mode == "create_or_replace":
            writer.createOrReplace()
            return
        if mode == "overwrite_partitions":
            table_exists = False
            if spark is not None and hasattr(spark, "catalog"):
                try:
                    table_exists = spark.catalog.tableExists(table_name)
                except Exception:
                    table_exists = False
            if table_exists:
                writer.overwritePartitions()
            else:
                try:
                    writer.create()
                except Exception:
                    writer.createOrReplace()
            return
        if mode == "append":
            writer.append()
            return
        raise ValueError(f"Unsupported Iceberg write mode: {mode}")
