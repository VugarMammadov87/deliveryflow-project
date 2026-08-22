from __future__ import annotations

"""Smoke test for Spark, Nessie, Iceberg, and S3-compatible storage wiring.

The job exists to validate infrastructure integration rather than business
logic. If this tiny Iceberg write/read succeeds, Spark can reach the Nessie
catalog and the configured warehouse storage used by the real batch job.
"""

from pyspark.sql import SparkSession


def main() -> None:
    """Create a minimal Iceberg table, write one row, and read it back."""
    spark = SparkSession.builder.appName("deliveryflow-iceberg-smoke-test").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.iceberg_smoke_test
        (
            id INT,
            message STRING
        )
        USING iceberg
        """
    )
    spark.sql("INSERT INTO nessie.bronze.iceberg_smoke_test VALUES (1, 'spark-nessie-iceberg-s3-ok')")
    rows = spark.sql("SELECT id, message FROM nessie.bronze.iceberg_smoke_test WHERE id = 1").collect()
    if not rows:
        raise RuntimeError("Iceberg smoke test table returned no rows")
    print("ICEBERG_SMOKE_TEST_OK")
    spark.stop()


if __name__ == "__main__":
    main()
