"""Environment-backed configuration for the synthetic producer.

Configuration is isolated from event generation and transport code so future
metadata-driven settings can be introduced without changing domain builders or
Kafka publishing behavior.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _is_enabled(value: str) -> bool:
    """Return whether an environment value represents an enabled flag."""
    return value.strip().lower() in {"1", "true", "yes"}


@dataclass(frozen=True)
class ProducerConfig:
    """Runtime settings for Kafka publishing and source seeding."""

    bootstrap_servers: str
    topic: str
    event_count: int
    interval_seconds: float
    mode: str
    batch_rows: int
    continuous: bool
    continuous_interval_seconds: float
    continuous_records_per_interval: int
    app: str
    fleet_topic: str

    def __post_init__(self) -> None:
        """Reject invalid settings before any external connection is opened."""
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        if not self.topic.strip() or not self.fleet_topic.strip():
            raise ValueError("Kafka topic names must not be empty")
        if self.event_count < 0:
            raise ValueError("PRODUCER_EVENT_COUNT must be greater than or equal to zero")
        if self.interval_seconds < 0:
            raise ValueError("PRODUCER_INTERVAL_SECONDS must be greater than or equal to zero")
        if self.batch_rows < 0:
            raise ValueError("PRODUCER_BATCH_ROWS must be greater than or equal to zero")
        if self.continuous_interval_seconds <= 0:
            raise ValueError("PRODUCER_CONTINUOUS_INTERVAL_SECONDS must be greater than zero")
        if self.continuous_records_per_interval <= 0:
            raise ValueError("PRODUCER_EVENTS_PER_INTERVAL must be greater than zero")
        if self.app not in {"delivery", "fleet"}:
            raise ValueError("PRODUCER_APP must be one of: delivery, fleet")
        if self.mode not in {"stream", "batch", "both"}:
            raise ValueError("PRODUCER_MODE must be one of: stream, batch, both")

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """Build settings from environment variables and local-safe defaults.

        Continuous mode emits a fixed-size batch at each interval boundary.
        The interval variable remains the public override, while records per
        interval controls throughput without coupling it to sleep timing.
        """
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "delivery-events"),
            event_count=int(os.getenv("PRODUCER_EVENT_COUNT", "25")),
            interval_seconds=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "0.2")),
            mode=os.getenv("PRODUCER_MODE", "both").lower(),
            batch_rows=int(os.getenv("PRODUCER_BATCH_ROWS", "40")),
            continuous=_is_enabled(os.getenv("PRODUCER_CONTINUOUS", "false")),
            continuous_interval_seconds=float(os.getenv("PRODUCER_CONTINUOUS_INTERVAL_SECONDS", "60")),
            continuous_records_per_interval=int(os.getenv("PRODUCER_EVENTS_PER_INTERVAL", "10")),
            app=os.getenv("PRODUCER_APP", "delivery").lower(),
            fleet_topic=os.getenv("KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS", "vehicle-telemetry-events"),
        )


@dataclass(frozen=True)
class SourceDbConfig:
    """Connection settings for the PostgreSQL operational source database."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    def __post_init__(self) -> None:
        """Reject incomplete PostgreSQL settings before connection attempts."""
        if not self.host.strip() or not self.dbname.strip() or not self.user.strip():
            raise ValueError("Source database host, name, and user must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("SOURCE_DB_PORT must be between 1 and 65535")

    @classmethod
    def from_env(cls) -> "SourceDbConfig":
        """Read PostgreSQL source settings from environment variables."""
        return cls(
            host=os.getenv("SOURCE_DB_HOST", "localhost"),
            port=int(os.getenv("SOURCE_DB_PORT", "15433")),
            dbname=os.getenv("SOURCE_DB_NAME", "logistics_source"),
            user=os.getenv("SOURCE_DB_USER", "source_app"),
            password=os.getenv("SOURCE_DB_PASSWORD", "local-source-password"),
        )
