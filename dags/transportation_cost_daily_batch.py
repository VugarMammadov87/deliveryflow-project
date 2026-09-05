from __future__ import annotations

"""Airflow registration for the transportation cost batch KPI pipeline."""

from datetime import datetime

from batch_spark_dag_factory import BatchSparkDatasetDagConfig, create_batch_spark_dataset_dag


dag = create_batch_spark_dataset_dag(
    BatchSparkDatasetDagConfig(
        dag_id="delivery_transportation_cost_daily_batch",
        description="Orchestrates transportation cost KPI processing from PostgreSQL to ClickHouse.",
        source_table="transportation_costs",
        source_date_column="business_date",
        spark_app_path="/opt/deliveryflow/src/etl/apps/transportation_cost_kpi_job.py",
        metadata_path="/opt/deliveryflow/configs/datasets/delivery/transportation_costs.yaml",
        clickhouse_table="daily_transportation_cost_kpi",
        start_date=datetime(2026, 9, 4),
        schedule="@daily",
    )
)
