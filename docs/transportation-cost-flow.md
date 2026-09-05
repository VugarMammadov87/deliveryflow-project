# Transportation Cost Batch Flow

This document describes Dashboard 6: **Transportation Cost & Route Performance**.

The flow simulates a practical daily batch ETL process for transportation cost and route performance analytics. It starts from synthetic business data written into PostgreSQL and ends with a Superset dashboard sourced from ClickHouse.

## End-to-End Flow

```text
Synthetic Transportation Data
           |
           v
PostgreSQL Source
           |
           v
Airflow
           |
           v
Spark
           |
           v
Iceberg Bronze
           |
           v
Iceberg Silver
           |
           v
Iceberg Gold
           |
           v
ClickHouse
           |
           v
Superset Dataset
           |
           v
Transportation Cost & Route Performance Dashboard
```

## Business Goal

The dashboard helps logistics managers understand daily transportation cost and route performance:

- Total transportation cost.
- Planned cost versus actual cost variance.
- Fuel share of transportation cost.
- Cost per delivery.
- Cost per kilometer.
- Warehouse and region cost contribution.
- Routes that combine high cost and high delay.
- Relationship between vehicle utilization and cost.

## PostgreSQL Source

Synthetic source rows are generated into:

```text
public.transportation_costs
```

The table stores route-level cost facts by business date, delivery, route, vehicle, warehouse, and region. The source records reuse delivery identifiers where possible so cost data can be analyzed alongside operational delivery events.

Important source rules:

- `cost_id` and `business_date` are required.
- Distances, cost components, liters, delivery counts, capacities, and durations must be non-negative.
- `actual_total_cost` must equal `fuel_cost + driver_cost + toll_cost + maintenance_cost + other_cost`.
- `used_capacity` cannot exceed `vehicle_capacity`.

Generate the PostgreSQL batch source:

```powershell
make seed-batch-source
```

## Airflow Orchestration

The Airflow DAG is:

```text
delivery_transportation_cost_daily_batch
```

The task sequence is:

```text
check_transport_cost_source
        |
        v
run_transport_cost_spark_job
        |
        v
validate_transport_cost_output
```

The DAG is built through `dags/batch_spark_dag_factory.py` so future batch datasets can follow the same source-check, Spark-submit, and serving-validation pattern.

## Spark ETL

The Spark application is:

```text
src/etl/apps/transportation_cost_kpi_job.py
```

Run it manually:

```powershell
make run-batch DATASET=transportation-costs
```

Run it for a specific business date:

```powershell
make run-batch DATASET=transportation-costs BATCH_DATE=2026-09-04
```

The Spark job performs:

- PostgreSQL source read using dataset metadata.
- Data quality validation.
- Bronze write.
- Silver enrichment.
- Gold aggregation.
- ClickHouse serving publication.

## Iceberg Layers

Bronze table:

```text
nessie.bronze.transportation_costs
```

Bronze keeps source-like transportation cost records and adds audit fields:

- `_ingested_at`
- `_pipeline_run_id`
- `_source_system`
- `_batch_date`

Silver table:

```text
nessie.silver.transportation_costs_enriched
```

Silver adds row-level derived metrics:

- `cost_variance = actual_total_cost - planned_cost`
- `cost_variance_pct = cost_variance / planned_cost`
- `distance_variance_km = actual_distance_km - planned_distance_km`
- `distance_variance_pct = distance_variance_km / planned_distance_km`
- `duration_variance_minutes = actual_duration_minutes - planned_duration_minutes`
- `cost_per_km = actual_total_cost / actual_distance_km`
- `cost_per_delivery = actual_total_cost / delivery_count`
- `fuel_cost_pct = fuel_cost / actual_total_cost`
- `vehicle_utilization_pct = used_capacity / vehicle_capacity`

Zero or null denominators return `0.0`.

Gold table:

```text
nessie.gold.daily_transportation_cost_kpi
```

Gold aggregates at this grain:

```text
business_date, warehouse_id, region, route_id
```

The table contains BI-ready daily metrics such as actual cost, planned cost, cost variance, cost per kilometer, cost per delivery, fuel share, average utilization, and average duration variance.

## ClickHouse Serving

Serving table:

```text
delivery.daily_transportation_cost_kpi
```

BI view:

```text
delivery.v_transportation_cost_performance
```

The view joins transportation cost KPI rows with delivery event delay metrics by date, warehouse, region, and route. This supports the management insight "high cost and high delay routes".

## Superset Dashboard

The Superset dashboard is managed as code in:

```text
configs/superset/deliveryflow_bi.yaml
```

Dashboard:

```text
Transportation Cost & Route Performance
```

Dataset:

```text
Transportation Cost Performance -> delivery.v_transportation_cost_performance
```

Charts:

- Actual Transportation Cost.
- Transportation Cost Variance.
- Transportation Cost per Delivery.
- Transportation Cost Breakdown.
- Route Cost Performance.
- Warehouse Region Transportation Cost.
- Route Cost vs Delay Risk.

Filters:

- Business Date (`business_date`)
- Warehouse (`warehouse_id`)
- Region (`region`)
- Route (`route_id`)

Import Superset assets:

```powershell
make import-superset-assets
```

## Practical Demo Sequence

```powershell
Copy-Item .env.example .env
make config
make up
make health
make submit-flink-job
make produce
make run-batch DATASET=transportation-costs
make import-superset-assets
make console
```

After the import, open Superset and navigate to:

```text
Transportation Cost & Route Performance
```

## Scalability Notes

This flow is intentionally metadata-backed:

- Dataset metadata lives under `configs/datasets/delivery/`.
- Airflow uses a reusable DAG factory.
- Spark uses shared metadata, quality, reader, writer, and publisher helpers.
- Superset assets remain version-controlled.

This keeps future batch dataset onboarding close to:

```text
Dataset metadata
        |
        v
Reusable Airflow pattern
        |
        v
Shared Spark framework
        |
        v
ClickHouse serving object
        |
        v
Superset BI-as-Code
```
