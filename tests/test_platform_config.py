from __future__ import annotations

"""Tests for DeliveryFlow central platform configuration loading.

These tests verify that platform service settings (Kafka topics, ClickHouse
endpoints, authentication parameters) resolve correctly from configuration files
and environment overrides.
"""

from deliveryflow_config import load_platform_config


def test_platform_config_provides_shared_topic_defaults(monkeypatch) -> None:
    """Verify app metadata can share central Kafka topic names."""
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC_DELIVERY_EVENTS", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", raising=False)

    kafka = load_platform_config().kafka()

    assert kafka.delivery_events_topic == "delivery-events"
    assert kafka.vehicle_telemetry_events_topic == "vehicle-telemetry-events"


def test_platform_config_keeps_clickhouse_secrets_in_environment(monkeypatch) -> None:
    """Verify secrets are runtime overrides, not non-secret config values."""
    monkeypatch.setenv("CLICKHOUSE_USER", "runtime_user")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "runtime_password")

    clickhouse = load_platform_config().clickhouse(application="fleet")

    assert clickhouse.database == "fleet"
    assert clickhouse.user == "runtime_user"
    assert clickhouse.password == "runtime_password"
