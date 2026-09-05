from __future__ import annotations

"""Reusable ClickHouse serving publication helpers."""

from dataclasses import dataclass
from typing import Iterable

from deliveryflow_config import ClickHouseConnection


@dataclass(frozen=True)
class ClickHousePublisher:
    """Publish small aggregated Spark result sets to ClickHouse serving tables."""

    connection: ClickHouseConnection

    def publish(self, dataframe: object, table_name: str, columns: list[str]) -> int:
        """Collect selected columns and insert them into a ClickHouse table."""
        rows = dataframe.select(*columns).collect()
        self.insert_rows(table_name, columns, (tuple(row) for row in rows))
        return len(rows)

    def insert_rows(self, table_name: str, columns: list[str], rows: Iterable[tuple[object, ...]]) -> None:
        """Insert already materialized rows into ClickHouse."""
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=self.connection.host,
            port=self.connection.http_port,
            username=self.connection.user,
            password=self.connection.password,
            database=self.connection.database,
        )
        try:
            client.insert(table_name, list(rows), column_names=columns)
        finally:
            client.close()
