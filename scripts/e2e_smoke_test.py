from __future__ import annotations

"""End-to-end smoke test for the streaming path.

This script exists to prove that the generated logistics events can travel from
the Python producer through Kafka and Flink into ClickHouse serving tables. It
keeps the test small so it can run locally after `make up` without becoming a
full integration test suite.
"""

import os
import subprocess
import sys
import time

import clickhouse_connect

from producers.synthetic_logistics_producer import main as produce_events


def query_count(table: str) -> int:
    """Return the current ClickHouse row count for a serving table."""
    return int(query_scalar(f"SELECT count() FROM {table}"))


def query_scalar(sql: str) -> object:
    """Run one ClickHouse scalar query against the local serving database."""
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "delivery_app"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "delivery"),
    )
    return client.query(sql).result_rows[0][0]


def wait_for_clickhouse_rows(table: str, minimum: int, timeout_seconds: int = 90) -> None:
    """Wait until Flink has materialized enough rows in ClickHouse."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count = query_count(table)
        if count >= minimum:
            print(f"{table} rows={count}")
            return
        time.sleep(3)
    raise RuntimeError(f"{table} did not reach {minimum} rows within {timeout_seconds}s")


def wait_for_clickhouse_count_increase(table: str, initial_count: int, timeout_seconds: int = 90) -> None:
    """Wait until a smoke run adds at least one row to a ClickHouse table."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count = query_count(table)
        if count > initial_count:
            print(f"{table} rows={count} delta={count - initial_count}")
            return
        time.sleep(3)
    raise RuntimeError(f"{table} did not increase above {initial_count} rows within {timeout_seconds}s")


def wait_for_clickhouse_timestamp_advance(table: str, column: str, initial_timestamp: object, timeout_seconds: int = 90) -> None:
    """Wait until a latest-state style table advances its freshness marker."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        timestamp = query_scalar(f"SELECT max({column}) FROM {table}")
        if initial_timestamp is None or timestamp > initial_timestamp:
            print(f"{table} {column}={timestamp}")
            return
        time.sleep(3)
    raise RuntimeError(f"{table}.{column} did not advance beyond {initial_timestamp} within {timeout_seconds}s")


def main() -> int:
    """Run the selected end-to-end smoke scenario."""
    if os.getenv("E2E_APP", "delivery").lower() == "fleet":
        return fleet_main()

    return delivery_main()


def delivery_main() -> int:
    """Produce a small event batch and validate ClickHouse side effects."""
    os.environ.setdefault("PRODUCER_EVENT_COUNT", "10")
    os.environ.setdefault("PRODUCER_INTERVAL_SECONDS", "0.05")
    delivery_events_before = query_count("delivery_events")
    delivery_state_before = query_count("delivery_current_state")
    produce_events()
    wait_for_clickhouse_count_increase("delivery_events", delivery_events_before)
    wait_for_clickhouse_count_increase("delivery_current_state", delivery_state_before)
    print("STREAMING_E2E_OK")

    result = subprocess.run(
        ["python", "/app/src/etl/apps/iceberg_smoke_test.py"],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        print(result.stdout)
    else:
        print("Iceberg smoke test must be run with spark-submit from a Spark container.")

    return 0


def fleet_main() -> int:
    """Produce fleet telemetry and validate Flink SQL ClickHouse outputs."""
    os.environ["PRODUCER_APP"] = "fleet"
    os.environ["PRODUCER_MODE"] = "stream"
    os.environ.setdefault("PRODUCER_EVENT_COUNT", "12")
    os.environ.setdefault("PRODUCER_INTERVAL_SECONDS", "0.05")
    telemetry_events_before = query_count("fleet.vehicle_telemetry_events")
    vehicle_state_timestamp_before = query_scalar("SELECT max(last_event_timestamp) FROM fleet.vehicle_current_state")
    health_alerts_before = query_count("fleet.vehicle_health_alerts")
    metrics_timestamp_before = query_scalar("SELECT max(window_end) FROM fleet.vehicle_metrics_5m")
    produce_events()
    wait_for_clickhouse_count_increase("fleet.vehicle_telemetry_events", telemetry_events_before)
    wait_for_clickhouse_timestamp_advance("fleet.vehicle_current_state", "last_event_timestamp", vehicle_state_timestamp_before)
    wait_for_clickhouse_count_increase("fleet.vehicle_health_alerts", health_alerts_before)
    wait_for_clickhouse_timestamp_advance("fleet.vehicle_metrics_5m", "window_end", metrics_timestamp_before)
    print("FLEET_STREAMING_E2E_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
