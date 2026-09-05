from __future__ import annotations

"""Reusable source readers for Spark batch jobs."""

from dataclasses import dataclass

from deliveryflow_config import PostgresConnection

from etl.framework.metadata import DatasetMetadata


@dataclass(frozen=True)
class PostgresReader:
    """Build and execute Spark JDBC reads from PostgreSQL source tables."""

    connection: PostgresConnection

    @property
    def jdbc_url(self) -> str:
        """Return the Spark JDBC URL for the configured PostgreSQL source."""
        return f"jdbc:postgresql://{self.connection.host}:{self.connection.port}/{self.connection.database}"

    def options_for(self, metadata: DatasetMetadata) -> dict[str, str]:
        """Return Spark JDBC options for a metadata-backed source table."""
        return {
            "url": self.jdbc_url,
            "dbtable": f"{metadata.postgres_schema}.{metadata.postgres_table}",
            "user": self.connection.user,
            "password": self.connection.password,
            "driver": "org.postgresql.Driver",
        }

    def read(self, spark: object, metadata: DatasetMetadata) -> object:
        """Read a metadata-backed PostgreSQL source table with Spark."""
        reader = spark.read.format("jdbc")
        for key, value in self.options_for(metadata).items():
            reader = reader.option(key, value)
        return reader.load()
