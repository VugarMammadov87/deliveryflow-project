from __future__ import annotations

"""Static tests for Airflow runtime packaging used by metadata-backed batch jobs."""

from pathlib import Path


AIRFLOW_DOCKERFILE = Path("services/airflow/Dockerfile")
COMPOSE_FILE = Path("docker-compose.yml")


def test_airflow_image_contains_dataset_metadata_configs() -> None:
    """Verify Airflow can read dataset metadata referenced by batch DAGs."""
    dockerfile = AIRFLOW_DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY --chown=airflow:0 configs/platform.yaml /opt/deliveryflow/configs/platform.yaml" in dockerfile
    assert "COPY --chown=airflow:0 configs/datasets /opt/deliveryflow/configs/datasets" in dockerfile
    assert "COPY --chown=airflow:0 src/etl /opt/deliveryflow/src/etl" in dockerfile


def test_airflow_environment_contains_source_database_connection() -> None:
    """Verify Airflow tasks can resolve the PostgreSQL source connection."""
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    airflow_env_start = compose.index("environment: &airflow-env")
    airflow_env_end = compose.index("user: \"50000:0\"", airflow_env_start)
    airflow_env = compose[airflow_env_start:airflow_env_end]

    assert "SOURCE_DB_HOST: postgres-source" in airflow_env
    assert "SOURCE_DB_PORT: 5432" in airflow_env
    assert "SOURCE_DB_NAME: ${SOURCE_DB_NAME}" in airflow_env
    assert "SOURCE_DB_USER: ${SOURCE_DB_USER}" in airflow_env
    assert "SOURCE_DB_PASSWORD: ${SOURCE_DB_PASSWORD}" in airflow_env
