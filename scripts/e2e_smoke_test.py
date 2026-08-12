from __future__ import annotations

import os
import subprocess
import sys
import time

import clickhouse_connect

from producers.synthetic_logistics_producer import main as produce_events


def query_count(table: str) -> int:
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "delivery_app"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password"),
        database=os.getenv("CLICKHOUSE_DATABASE", "delivery"),
    )
    return int(client.query(f"SELECT count() FROM {table}").result_rows[0][0])


def wait_for_clickhouse_rows(table: str, minimum: int, timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count = query_count(table)
        if count >= minimum:
            print(f"{table} rows={count}")
            return
        time.sleep(3)
    raise RuntimeError(f"{table} did not reach {minimum} rows within {timeout_seconds}s")


def main() -> int:
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


if __name__ == "__main__":
    sys.exit(main())
