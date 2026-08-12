from __future__ import annotations

import os
import socket
import sys
import time
from dataclasses import dataclass

import clickhouse_connect
import requests
from kafka import KafkaConsumer


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def tcp_check(name: str, host: str, port: int, timeout: float = 3.0) -> Check:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return Check(name, True, f"{host}:{port} reachable")
    except OSError as exc:
        return Check(name, False, str(exc))


def http_check(name: str, url: str, timeout: float = 5.0) -> Check:
    try:
        response = requests.get(url, timeout=timeout)
        return Check(name, response.ok, f"HTTP {response.status_code}")
    except requests.RequestException as exc:
        return Check(name, False, str(exc))


def kafka_check() -> Check:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        topics = consumer.topics()
        consumer.close()
        return Check("Kafka", "delivery-events" in topics, f"topics={sorted(topics)}")
    except Exception as exc:
        return Check("Kafka", False, str(exc))


def clickhouse_check() -> Check:
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
    checks = [
        kafka_check(),
        tcp_check("PostgresAirflow", "postgres-airflow", 5432),
        tcp_check("PostgresSource", "postgres-source", 5432),
        http_check("MinIO", "http://minio:9000/minio/health/ready"),
        http_check("Nessie", "http://nessie:19120/api/v2/config"),
        http_check("Spark", "http://spark-master:8080"),
        clickhouse_check(),
        http_check("Flink", "http://flink-jobmanager:8081/overview"),
        http_check("Airflow", "http://airflow-webserver:8080/health"),
    ]
    for check in checks:
        status = "healthy" if check.ok else "unhealthy"
        print(f"{check.name:14} {status:10} {check.detail}")
    return 0 if all(check.ok for check in checks) else 1


if __name__ == "__main__":
    time.sleep(1)
    sys.exit(main())
