from __future__ import annotations

"""Central platform connection configuration for DeliveryFlow applications."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


CONFIG_PATH_ENV = "DELIVERYFLOW_CONFIG_PATH"


def _candidate_paths() -> list[Path]:
    configured = os.getenv(CONFIG_PATH_ENV) or os.getenv("PLATFORM_CONFIG_PATH")
    paths = [
        Path(configured) if configured else None,
        Path("/app/configs/platform.yaml"),
        Path("/opt/deliveryflow/configs/platform.yaml"),
        Path.cwd() / "configs" / "platform.yaml",
    ]
    return [path for path in paths if path is not None]


def _read_config() -> dict[str, Any]:
    for path in _candidate_paths():
        if path.exists():
            with path.open("r", encoding="utf-8") as stream:
                payload = yaml.safe_load(stream) or {}
            platform = payload.get("platform", payload)
            if not isinstance(platform, dict):
                raise ValueError(f"Invalid platform config shape in {path}")
            return platform
    checked = ", ".join(str(path) for path in _candidate_paths())
    raise FileNotFoundError(f"DeliveryFlow platform config not found. Checked: {checked}")


def _section(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    value: Any = config
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            raise KeyError("Missing platform config section: " + ".".join(keys))
        value = value[key]
    if not isinstance(value, dict):
        raise ValueError("Platform config section is not an object: " + ".".join(keys))
    return value


def _env_or_value(env_name: str | None, fallback: Any) -> str:
    if env_name:
        value = os.getenv(env_name)
        if value is not None and value != "":
            return value
    return str(fallback)


@dataclass(frozen=True)
class KafkaConnection:
    bootstrap_servers: str
    delivery_events_topic: str
    vehicle_telemetry_events_topic: str


@dataclass(frozen=True)
class ClickHouseConnection:
    host: str
    http_port: int
    http_url: str
    database: str
    user: str
    password: str
    jdbc_driver: str = "com.clickhouse.jdbc.ClickHouseDriver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:clickhouse://{self.host}:{self.http_port}/{self.database}"


@dataclass(frozen=True)
class PostgresConnection:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class SupersetConnection:
    base_url: str
    username: str
    password: str
    asset_file: Path


@dataclass(frozen=True)
class PlatformConfig:
    raw: dict[str, Any]

    @classmethod
    def load(cls) -> "PlatformConfig":
        return cls(raw=_read_config())

    def kafka(self) -> KafkaConnection:
        kafka = _section(self.raw, "kafka")
        topics = _section(kafka, "topics")
        return KafkaConnection(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                str(kafka["internal_bootstrap_servers"]),
            ),
            delivery_events_topic=os.getenv(
                "KAFKA_TOPIC",
                os.getenv("KAFKA_TOPIC_DELIVERY_EVENTS", str(topics["delivery_events"])),
            ),
            vehicle_telemetry_events_topic=os.getenv(
                "KAFKA_TOPIC_VEHICLE_TELEMETRY_EVENTS",
                str(topics["vehicle_telemetry_events"]),
            ),
        )

    def clickhouse(self, *, application: str = "delivery") -> ClickHouseConnection:
        clickhouse = _section(self.raw, "clickhouse")
        databases = _section(clickhouse, "databases")
        default_database = databases["fleet"] if application == "fleet" else databases["delivery"]
        database_env = "FLEET_CLICKHOUSE_DATABASE" if application == "fleet" else "CLICKHOUSE_DATABASE"
        return ClickHouseConnection(
            host=os.getenv("CLICKHOUSE_HOST", str(clickhouse["host"])),
            http_port=int(os.getenv("CLICKHOUSE_HTTP_PORT", str(clickhouse["http_port"]))),
            http_url=os.getenv("CLICKHOUSE_URL", str(clickhouse["http_url"])),
            database=os.getenv(database_env, str(default_database)),
            user=_env_or_value(clickhouse.get("username_env"), ""),
            password=_env_or_value(clickhouse.get("password_env"), ""),
        )

    def postgres(self, name: str = "source") -> PostgresConnection:
        postgres = _section(self.raw, "postgres", name)
        return PostgresConnection(
            host=os.getenv(f"{name.upper()}_DB_HOST", str(postgres["host"])),
            port=int(os.getenv(f"{name.upper()}_DB_PORT", str(postgres["port"]))),
            database=_env_or_value(postgres.get("database_env"), ""),
            user=_env_or_value(postgres.get("username_env"), ""),
            password=_env_or_value(postgres.get("password_env"), ""),
        )

    def superset(self) -> SupersetConnection:
        superset = _section(self.raw, "superset")
        return SupersetConnection(
            base_url=os.getenv("SUPERSET_BASE_URL", str(superset["base_url"])).rstrip("/"),
            username=_env_or_value(superset.get("username_env"), ""),
            password=_env_or_value(superset.get("password_env"), ""),
            asset_file=Path(os.getenv("SUPERSET_ASSET_FILE", str(superset["asset_file"]))),
        )

    def endpoint(self, section_name: str, key: str = "endpoint") -> str:
        section = _section(self.raw, section_name)
        return str(section[key])


def load_platform_config() -> PlatformConfig:
    return PlatformConfig.load()
