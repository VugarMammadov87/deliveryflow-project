from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="delivery_daily_kpi",
    description="Orchestrates Spark daily logistics KPI processing.",
    start_date=datetime(2026, 8, 8),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "deliveryflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["deliveryflow", "spark", "iceberg", "clickhouse"],
) as dag:
    check_source_readiness = BashOperator(
        task_id="check_source_readiness",
        bash_command=(
            "python - <<'PY'\n"
            "import clickhouse_connect\n"
            "client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='delivery_app', password='local-clickhouse-password', database='delivery')\n"
            "count = client.query('SELECT count() FROM delivery_events').result_rows[0][0]\n"
            "assert count > 0, 'delivery_events is empty'\n"
            "print(f'delivery_events rows={count}')\n"
            "PY"
        ),
    )

    run_spark_daily_batch = BashOperator(
        task_id="run_spark_daily_batch",
        bash_command="spark-submit --master spark://spark-master:7077 /opt/deliveryflow/src/etl/apps/daily_kpi_job.py",
    )

    check_source_readiness >> run_spark_daily_batch
