from __future__ import annotations

"""Non-destructive runtime health checks for the local DeliveryFlow platform.

This script exists so `make health` can validate the full Docker Compose stack
without mutating business data. It checks service reachability and simple API
responses, then returns a non-zero exit code when any required service is not
ready.
"""

import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Callable

import clickhouse_connect
import requests
from kafka import KafkaConsumer


@dataclass(frozen=True)
class Check:
    """Result object shared by all health checks.

    A common shape makes the final report easy to scan and keeps individual
    checks focused on one service-specific question.
    """

    name: str
    ok: bool
    detail: str


def tcp_check(name: str, host: str, port: int, timeout: float = 3.0) -> Check:
    """Check raw TCP reachability for services without cheap HTTP endpoints."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return Check(name, True, f"{host}:{port} reachable")
    except OSError as exc:
        return Check(name, False, str(exc))


def http_check(name: str, url: str, timeout: float = 5.0) -> Check:
    """Check HTTP readiness endpoints used by UI and API services."""
    try:
        response = requests.get(url, timeout=timeout)
        return Check(name, response.ok, f"HTTP {response.status_code}")
    except requests.RequestException as exc:
        return Check(name, False, str(exc))


def retry_check(factory: Callable[[], Check], max_wait_seconds: float, interval_seconds: float) -> Check:
    """Retry a check while containers are still moving from started to ready."""
    deadline = time.time() + max_wait_seconds
    last_check = factory()
    while not last_check.ok and time.time() < deadline:
        time.sleep(interval_seconds)
        last_check = factory()
    return last_check


def kafka_check() -> Check:
    """Verify Kafka is reachable and the expected delivery topic exists."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        topics = consumer.topics()
        consumer.close()
        required_topics = {
            os.getenv("KAFKA_TOPIC", "delivery-events"),
            os.getenv("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", "vehicle-telemetry-events"),
        }
        missing_topics = required_topics.difference(topics)
        return Check("Kafka", not missing_topics, f"topics={sorted(topics)} missing={sorted(missing_topics)}")
    except Exception as exc:
        return Check("Kafka", False, str(exc))


def clickhouse_check() -> Check:
    """Verify ClickHouse accepts authenticated analytical queries."""
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "delivery_app"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "local-clickhouse-password"),
            database=os.getenv("CLICKHOUSE_DATABASE", "delivery"),
        )
        result = client.query("SELECT 1").result_rows[0][0]
        return Check("ClickHouse", result == 1, "SELECT 1 succeeded")
    except Exception as exc:
        return Check("ClickHouse", False, str(exc))


def main() -> int:
    """Run all platform checks and return a shell-friendly health status."""
    startup_wait_seconds = float(os.getenv("HEALTH_STARTUP_WAIT_SECONDS", "180"))
    retry_interval_seconds = float(os.getenv("HEALTH_RETRY_INTERVAL_SECONDS", "5"))
    checks = [
        retry_check(kafka_check, startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: tcp_check("PostgresAirflow", "postgres-airflow", 5432), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: tcp_check("PostgresSource", "postgres-source", 5432), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("MinIO", "http://minio:9000/minio/health/ready"), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("Nessie", "http://nessie:19120/api/v2/config"), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("Spark", "http://spark-master:8080"), startup_wait_seconds, retry_interval_seconds),
        retry_check(clickhouse_check, startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("Superset", "http://superset:8088/health"), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("Flink", "http://flink-jobmanager:8081/overview"), startup_wait_seconds, retry_interval_seconds),
        retry_check(lambda: http_check("Airflow", "http://airflow-webserver:8080/health"), startup_wait_seconds, retry_interval_seconds),
    ]
    for check in checks:
        status = "healthy" if check.ok else "unhealthy"
        print(f"{check.name:14} {status:10} {check.detail}")
    return 0 if all(check.ok for check in checks) else 1


if __name__ == "__main__":
    time.sleep(1)
    sys.exit(main())
