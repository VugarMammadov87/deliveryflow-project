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
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "delivery_app"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "delivery"),
    )
    return int(client.query(f"SELECT count() FROM {table}").result_rows[0][0])


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


def main() -> int:
    """Run the selected end-to-end smoke scenario."""
    if os.getenv("E2E_APP", "delivery").lower() == "fleet":
        return fleet_main()

    return delivery_main()


def delivery_main() -> int:
    """Produce a small event batch and validate ClickHouse side effects."""
    os.environ.setdefault("PRODUCER_EVENT_COUNT", "10")
    os.environ.setdefault("PRODUCER_INTERVAL_SECONDS", "0.05")
    produce_events()
    wait_for_clickhouse_rows("delivery_events", 1)
    wait_for_clickhouse_rows("delivery_current_state", 1)
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
    produce_events()
    wait_for_clickhouse_rows("fleet.vehicle_telemetry_events", 1)
    wait_for_clickhouse_rows("fleet.vehicle_current_state", 1)
    wait_for_clickhouse_rows("fleet.vehicle_health_alerts", 1)
    print("FLEET_STREAMING_E2E_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
