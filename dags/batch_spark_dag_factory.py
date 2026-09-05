from __future__ import annotations

"""Reusable Airflow DAG factory for DeliveryFlow Spark batch datasets."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


AIRFLOW_DS = "{{ ds }}"
AIRFLOW_RUN_ID = "{{ run_id }}"


@dataclass(frozen=True)
class BatchSparkDatasetDagConfig:
    """Configuration needed to orchestrate one metadata-backed Spark batch dataset."""

    dag_id: str
    description: str
    source_table: str
    source_date_column: str
    spark_app_path: str
    metadata_path: str
    clickhouse_table: str
    start_date: datetime = datetime(2026, 9, 4)
    schedule: str = "@daily"
    tags: list[str] = field(default_factory=lambda: ["deliveryflow", "batch", "spark", "iceberg", "clickhouse"])
    check_source_task_id: str = "check_transport_cost_source"
    run_spark_task_id: str = "run_transport_cost_spark_job"
    validate_output_task_id: str = "validate_transport_cost_output"


def create_batch_spark_dataset_dag(config: BatchSparkDatasetDagConfig) -> DAG:
    """Create a standard source-to-serving Airflow DAG for one Spark batch dataset."""
    default_args = {
        "owner": "deliveryflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    }

    with DAG(
        dag_id=config.dag_id,
        description=config.description,
        start_date=config.start_date,
        schedule=config.schedule,
        catchup=False,
        default_args=default_args,
        tags=config.tags,
    ) as dag:
        check_source = BashOperator(
            task_id=config.check_source_task_id,  # task_id="check_transport_cost_source"
            bash_command=_source_check_command(config.source_table, config.source_date_column),
            append_env=True,
        )

        run_spark = BashOperator(
            task_id=config.run_spark_task_id,  # task_id="run_transport_cost_spark_job"
            bash_command=_spark_submit_command(config.spark_app_path, config.metadata_path),
            append_env=True,
        )

        validate_output = BashOperator(
            task_id=config.validate_output_task_id,  # task_id="validate_transport_cost_output"
            bash_command=_clickhouse_validation_command(config.clickhouse_table),
            append_env=True,
        )

        check_source >> run_spark >> validate_output

    return dag


def _source_check_command(source_table: str, date_column: str) -> str:
    """Build a PostgreSQL source readiness check command."""
    return f"""set -euo pipefail
export SOURCE_DB_HOST="${{SOURCE_DB_HOST:-postgres-source}}"
export SOURCE_DB_PORT="${{SOURCE_DB_PORT:-5432}}"
export SOURCE_DB_NAME="${{SOURCE_DB_NAME:-logistics_source}}"
export SOURCE_DB_USER="${{SOURCE_DB_USER:-source_app}}"
export SOURCE_DB_PASSWORD="${{SOURCE_DB_PASSWORD:-local-source-password}}"
export BATCH_DATE="{AIRFLOW_DS}"
python - <<'PY'
import os

from deliveryflow_config import load_platform_config
import psycopg2

batch_date = os.environ["BATCH_DATE"]
platform = load_platform_config()
source = platform.postgres("source")
connection = psycopg2.connect(
    host=source.host,
    port=source.port,
    dbname=source.database,
    user=source.user,
    password=source.password,
)
try:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM {source_table} WHERE {date_column} = %s::date",
            (batch_date,),
        )
        count = cursor.fetchone()[0]
        if count <= 0:
            raise RuntimeError("No source rows found for {source_table} on " + batch_date)
        print("{source_table} rows=" + str(count))
finally:
    connection.close()
PY"""


def _spark_submit_command(spark_app_path: str, metadata_path: str) -> str:
    """Build the Spark submission command for a batch dataset."""
    return f"""set -euo pipefail
export SOURCE_DB_HOST="${{SOURCE_DB_HOST:-postgres-source}}"
export SOURCE_DB_PORT="${{SOURCE_DB_PORT:-5432}}"
export SOURCE_DB_NAME="${{SOURCE_DB_NAME:-logistics_source}}"
export SOURCE_DB_USER="${{SOURCE_DB_USER:-source_app}}"
export SOURCE_DB_PASSWORD="${{SOURCE_DB_PASSWORD:-local-source-password}}"
export TRANSPORTATION_COST_METADATA_PATH="{metadata_path}"
export PIPELINE_RUN_ID="{AIRFLOW_RUN_ID}"
export BATCH_DATE="{AIRFLOW_DS}"
spark-submit --master spark://spark-master:7077 {spark_app_path}"""


def _clickhouse_validation_command(clickhouse_table: str) -> str:
    """Build a ClickHouse serving-table validation command."""
    return f"""set -euo pipefail
export BATCH_DATE="{AIRFLOW_DS}"
python - <<'PY'
import os

from deliveryflow_config import load_platform_config
import clickhouse_connect

batch_date = os.environ["BATCH_DATE"]
platform = load_platform_config()
clickhouse = platform.clickhouse(application="delivery")
client = clickhouse_connect.get_client(
    host=clickhouse.host,
    port=clickhouse.http_port,
    username=clickhouse.user,
    password=clickhouse.password,
    database=clickhouse.database,
)
try:
    result = client.query(
        "SELECT count() FROM {clickhouse_table} WHERE business_date = toDate(%(batch_date)s)",
        parameters={{"batch_date": batch_date}},
    )
    count = result.result_rows[0][0]
    if count <= 0:
        raise RuntimeError("No ClickHouse KPI rows found for {clickhouse_table} on " + batch_date)
    print("{clickhouse_table} rows=" + str(count))
finally:
    client.close()
PY"""
