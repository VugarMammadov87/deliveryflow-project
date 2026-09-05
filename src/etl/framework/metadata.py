from __future__ import annotations

"""Dataset metadata loading for batch ETL jobs."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class DatasetMetadata:
    """Validated subset of a DeliveryFlow dataset metadata file."""

    dataset_id: str
    application: str
    domain: str
    source: dict[str, Any]
    ingestion: dict[str, Any]
    targets: dict[str, Any]
    quality: dict[str, Any]

    @classmethod
    def from_file(cls, path: str | Path) -> "DatasetMetadata":
        """Load and validate one dataset metadata YAML file."""
        metadata_path = Path(path)
        payload = yaml.safe_load(metadata_path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError(f"Dataset metadata must be a YAML object: {metadata_path}")
        return cls.from_dict(payload, source_path=metadata_path)

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, source_path: Path | None = None) -> "DatasetMetadata":
        """Validate required metadata sections from an in-memory payload."""
        location = f" in {source_path}" if source_path else ""
        required_keys = ["dataset_id", "application", "domain", "source", "ingestion", "targets"]
        missing = [key for key in required_keys if key not in payload]
        if missing:
            raise ValueError(f"Dataset metadata missing required keys{location}: {', '.join(missing)}")

        for section in ["source", "ingestion", "targets"]:
            if not isinstance(payload[section], dict):
                raise ValueError(f"Dataset metadata section must be an object{location}: {section}")

        return cls(
            dataset_id=str(payload["dataset_id"]),
            application=str(payload["application"]),
            domain=str(payload["domain"]),
            source=payload["source"],
            ingestion=payload["ingestion"],
            targets=payload["targets"],
            quality=payload.get("quality", {}),
        )

    @property
    def postgres_table(self) -> str:
        """Return the source PostgreSQL table name for batch readers."""
        if self.source.get("type") != "postgres":
            raise ValueError(f"Dataset {self.dataset_id} is not a PostgreSQL source")
        return str(self.source["table"])

    @property
    def postgres_schema(self) -> str:
        """Return the source PostgreSQL schema, defaulting to public."""
        return str(self.source.get("schema", "public"))

    @property
    def primary_key(self) -> list[str]:
        """Return merge primary-key columns from ingestion metadata."""
        primary_key = self.ingestion.get("primary_key", [])
        if not isinstance(primary_key, list) or not primary_key:
            raise ValueError(f"Dataset {self.dataset_id} must define ingestion.primary_key")
        return [str(column) for column in primary_key]

    @property
    def iceberg_targets(self) -> dict[str, str]:
        """Return configured Iceberg table targets."""
        iceberg = self.targets.get("iceberg", {})
        if not isinstance(iceberg, dict):
            raise ValueError(f"Dataset {self.dataset_id} targets.iceberg must be an object")
        return {str(key): str(value) for key, value in iceberg.items()}

    @property
    def clickhouse_target(self) -> dict[str, str]:
        """Return configured ClickHouse serving target metadata."""
        clickhouse = self.targets.get("clickhouse", {})
        if not isinstance(clickhouse, dict):
            raise ValueError(f"Dataset {self.dataset_id} targets.clickhouse must be an object")
        return {str(key): str(value) for key, value in clickhouse.items()}
