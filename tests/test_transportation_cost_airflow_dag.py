from __future__ import annotations

"""Static tests for the transportation cost Airflow orchestration files."""

from pathlib import Path


FACTORY = Path("dags/batch_spark_dag_factory.py")
DAG = Path("dags/transportation_cost_daily_batch.py")


def test_transportation_cost_dag_registers_metadata_backed_batch_pipeline() -> None:
    """Verify the DAG registration uses the reusable batch Spark factory."""
    dag_source = DAG.read_text(encoding="utf-8")

    assert "create_batch_spark_dataset_dag" in dag_source
    assert 'dag_id="delivery_transportation_cost_daily_batch"' in dag_source
    assert 'source_table="transportation_costs"' in dag_source
    assert 'spark_app_path="/opt/deliveryflow/src/etl/apps/transportation_cost_kpi_job.py"' in dag_source
    assert 'metadata_path="/opt/deliveryflow/configs/datasets/delivery/transportation_costs.yaml"' in dag_source
    assert 'clickhouse_table="daily_transportation_cost_kpi"' in dag_source


def test_batch_spark_factory_declares_expected_orchestration_tasks() -> None:
    """Verify the reusable factory creates the source, Spark, and serving validation tasks."""
    factory_source = FACTORY.read_text(encoding="utf-8")

    assert "class BatchSparkDatasetDagConfig" in factory_source
    assert 'task_id="check_transport_cost_source"' in factory_source
    assert 'task_id="run_transport_cost_spark_job"' in factory_source
    assert 'task_id="validate_transport_cost_output"' in factory_source
    assert "check_source >> run_spark >> validate_output" in factory_source


def test_batch_spark_factory_passes_runtime_batch_context() -> None:
    """Verify Airflow date and run id are passed into source checks and Spark."""
    factory_source = FACTORY.read_text(encoding="utf-8")

    assert 'AIRFLOW_DS = "{{ ds }}"' in factory_source
    assert 'AIRFLOW_RUN_ID = "{{ run_id }}"' in factory_source
    assert 'export BATCH_DATE="{AIRFLOW_DS}"' in factory_source
    assert 'export PIPELINE_RUN_ID="{AIRFLOW_RUN_ID}"' in factory_source
    assert "SOURCE_DB_HOST" in factory_source
    assert "spark-submit --master spark://spark-master:7077" in factory_source
    assert "SELECT count(*) FROM {source_table}" in factory_source
    assert "SELECT count() FROM {clickhouse_table}" in factory_source
