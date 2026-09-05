# DeliveryFlow Local Data Platform

## Multi-Application Stream Architecture Update

DeliveryFlow is now being built as a platform that can support multiple applications rather than only a single delivery-stream demo. The default operator flow remains unchanged:

```powershell
Copy-Item .env.example .env
make config
make up
make health
make submit-flink-job
make produce
make spark-daily-kpi
make run-batch DATASET=transportation-costs
make import-superset-assets
make console
```

Fleet vehicle telemetry is a second stream application and runs independently without changing the delivery job:

```powershell
make submit-flink-job APP=fleet
make produce APP=fleet
make test-e2e APP=fleet
```

This separation is important at a scale of 300-500 tables: application metadata is stored under `configs/applications/`, dataset metadata under `configs/datasets/`, and JSON contracts under `src/contracts/<domain>/`. The `delivery-events` topic carries delivery lifecycle events, while `vehicle-telemetry-events` carries fleet telemetry. ClickHouse follows the same ownership boundary: `delivery.*` serves delivery operations and `fleet.*` serves fleet telemetry.

Fleet telemetry assets:

- `configs/applications/fleet.yaml`
- `configs/datasets/fleet/vehicle_telemetry.yaml`
- `src/contracts/fleet/vehicle_telemetry_v1.schema.json`
- `services/flink/src/main/java/local/deliveryflow/VehicleTelemetrySqlJob.java`
- `fleet.vehicle_telemetry_events`
- `fleet.vehicle_current_state`
- `fleet.vehicle_health_alerts`
- `fleet.vehicle_metrics_5m`

DeliveryFlow is a data engineering project built on local Docker Compose. It demonstrates both real-time delivery monitoring and daily batch analytics for the logistics domain.

This README is intended for first-time users. It explains where to start, what each service does, the order in which `make` commands are run, and how to verify the pipelines.

## Brief Summary

The project has two primary data flows:
## Architecture Concepts

- **Stream pipeline**: `Producer -> Kafka -> Flink -> ClickHouse -> Superset`
- **Batch pipeline**: `ClickHouse/PostgreSQL -> Spark -> Iceberg/Nessie/MinIO -> ClickHouse -> Superset`
- **Transportation cost batch pipeline**: `PostgreSQL -> Airflow -> Spark -> Iceberg Bronze/Silver/Gold -> ClickHouse -> Superset`
When exploring DeliveryFlow, these core technologies work together across the streaming and batch layers:

Primary service roles:
- **Kafka**: Receives events from the producer and acts as the distributed event transport layer.
- **Flink**: Consumes events from Kafka and processes them in real time using event-time semantics.
- **ClickHouse**: Serves as the operational and BI serving database for fast analytical queries.
- **Apache Iceberg**: The open table format used for analytical datasets in the Data Lake.
- **MinIO**: Stores the physical Parquet data files and Iceberg metadata in local S3-compatible object storage.
- **Nessie**: Acts as the transactional Iceberg catalog and tracks table metadata references and git-like branches.
- **Spark**: Reads and writes Iceberg tables and calculates daily batch KPIs.
- **Airflow**: Orchestrates scheduled batch workflows and triggers Spark jobs.
- **PostgreSQL**: Stores metadata for Airflow and Nessie, and hosts relational source tables for batch seeding.
- **Superset**: Connects to ClickHouse to provide operational BI dashboards and reports.

- **Kafka** is the event transport layer.
- **Flink** performs real-time stream processing.
- **ClickHouse** is the operational and BI serving database.
- **Spark** calculates batch KPIs.
- **Iceberg** provides the analytical table format.
- **Nessie** is the Iceberg catalog.
- **MinIO** provides local S3-compatible object storage.
- **Airflow** orchestrates batch jobs.
- **PostgreSQL** stores Airflow, Nessie, and source metadata.
- **Superset** provides dashboards and reporting.
### Practical Data Flow Examples

**Application 1: Delivery Operations (Default Stream Pipeline)**

```text
delivery-events -> Flink DataStream -> delivery.*
```

Example flow:
1. A delivery event is created by the producer: `VEHICLE_DEPARTED`.
2. The event is published to the `delivery-events` Kafka topic.
3. Kafka stores the event and Flink consumes it.
4. Flink processes the event, validates the schema contract, and updates `delivery.delivery_current_state` in ClickHouse.
5. Superset queries ClickHouse to display the latest active delivery status on the dashboard.

**Application 2: Fleet Telemetry (Multi-Application Stream Pipeline)**

```text
vehicle-telemetry-events -> Flink SQL -> fleet.*
```

Example flow:
1. A telemetry event is created by the Fleet producer: `{"vehicle_id": "TRK-101", "speed_kmh": 88.5, "engine_temp_c": 98.2}`.
2. The event is published to the `vehicle-telemetry-events` Kafka topic.
3. Flink SQL consumes the event, assigns event-time watermarks, computes 5-minute aggregations, and evaluates health alert conditions.
4. Flink writes the current state to `fleet.vehicle_current_state` and alerts to `fleet.vehicle_health_alerts` in ClickHouse.

## Getting Started

If you are opening the project for the first time, follow this sequence:

1. Confirm that you are in the repository root.
2. Create `.env` from `.env.example`.
3. Validate the Docker Compose configuration.
4. Start the platform.
5. Check service health.
6. Submit the Flink stream job.
7. Generate synthetic data.
8. Verify stream results in ClickHouse.
9. Run the Spark batch KPI job.
10. Import the Superset BI-as-code objects.
11. Use the Superset report views in the dashboard.

Primary command flow:

```powershell
Copy-Item .env.example .env
make config
make up
make health
make submit-flink-job
make produce
make spark-daily-kpi
make run-batch DATASET=transportation-costs
make import-superset-assets
make console
```

## Architecture Diagram

```mermaid
flowchart LR
    producer["Synthetic Logistics Producer<br/>Python"]
    postgres["PostgreSQL<br/>Source Tables"]
    kafka["Kafka<br/>delivery-events topic"]
    flink["Flink DataStream Job<br/>Java"]
    clickhouse["ClickHouse<br/>Serving Layer"]
    spark["Spark Daily KPI Job<br/>PySpark"]
    transportSpark["Spark Transportation Cost Job<br/>PySpark"]
    iceberg["Apache Iceberg Tables<br/>bronze / silver / gold"]
    nessie["Nessie<br/>Iceberg Catalog"]
    minio["MinIO<br/>S3-compatible Storage"]
    airflow["Airflow<br/>Batch Orchestration"]
    superset["Superset<br/>BI Dashboards"]

    producer -->|batch seed| postgres
    producer -->|stream events| kafka
    kafka --> flink
    flink --> clickhouse
    airflow --> spark
    airflow --> transportSpark
    clickhouse --> spark
    postgres --> transportSpark
    spark --> iceberg
    transportSpark --> iceberg
    iceberg --> nessie
    iceberg --> minio
    spark --> clickhouse
    transportSpark --> clickhouse
    clickhouse --> superset
```

## Runtime Service Map

```mermaid
flowchart TB
    subgraph compose["Docker Compose: deliveryflow"]
        subgraph streaming["Streaming Layer"]
            kafka["Kafka Broker"]
            kafkaUi["Kafka UI"]
            producer["Producer Application<br/>minimal runtime image"]
            flinkJm["Flink JobManager"]
            flinkTm["Flink TaskManager"]
            flinkSubmit["Flink Job Submitter"]
        end

        subgraph developerTools["One-Shot Developer Tooling"]
            platformTools["Platform Tools<br/>health / E2E"]
            supersetImporter["Superset Importer"]
        end

        subgraph serving["Serving Layer"]
            clickhouse["ClickHouse"]
            superset["Superset"]
        end

        subgraph lakehouse["Lakehouse Layer"]
            sparkMaster["Spark Master"]
            sparkWorker["Spark Worker"]
            minio["MinIO"]
            minioInit["MinIO Init"]
            nessie["Nessie"]
        end

        subgraph orchestration["Orchestration Layer"]
            airflowDb["PostgreSQL<br/>Airflow Metadata"]
            airflowInit["Airflow Init"]
            airflowWeb["Airflow Webserver"]
            airflowScheduler["Airflow Scheduler"]
        end

        subgraph source["Source / Catalog Metadata"]
            postgresSource["PostgreSQL<br/>Source + Nessie Metadata"]
        end
    end

    producer --> kafka
    producer --> postgresSource
    platformTools -. health and E2E .-> kafka
    platformTools -. health and E2E .-> clickhouse
    supersetImporter -. BI import .-> superset
    kafka --> kafkaUi
    kafka --> flinkJm
    flinkJm --> flinkTm
    flinkSubmit --> flinkJm
    flinkJm --> clickhouse
    superset --> clickhouse
    airflowScheduler --> sparkMaster
    sparkMaster --> sparkWorker
    sparkMaster --> clickhouse
    sparkMaster --> minio
    sparkMaster --> nessie
    nessie --> postgresSource
    airflowWeb --> airflowDb
    airflowScheduler --> airflowDb
    minioInit --> minio
    airflowInit --> airflowDb
```

## Repository Structure

```text
.
|-- configs/
|   `-- spark/
|       `-- spark-defaults.conf
|-- dags/
|   `-- delivery_daily_kpi.py
|-- docs/
|   |-- architecture-decisions.md
|   |-- application-workflow.md
|   |-- postgres-source-tables.md
|   `-- superset-serving.md
|-- scripts/
|   |-- e2e_smoke_test.py
|   `-- health_check.py
|-- services/
|   |-- airflow/
|   |-- clickhouse/
|   |-- flink/
|   |-- kafka/
|   |-- postgres/
|   |-- producer/
|   |-- spark/
|   |-- superset/
|   `-- tooling/
|-- src/
|   |-- contracts/
|   |   `-- delivery_event_v1.schema.json
|   |-- etl/
|   |   `-- apps/
|   |       |-- daily_kpi_job.py
|   |       `-- iceberg_smoke_test.py
|   `-- producers/
|       |-- config.py
|       |-- events.py
|       |-- source.py
|       |-- streaming.py
|       `-- synthetic_logistics_producer.py
|-- tests/
|   |-- test_event_contract.py
|   `-- test_producer_rate.py
|-- docker-compose.yml
|-- Makefile
|-- README.md
|-- requirement.md
`-- plan.md
```

## Prerequisites

Local operation requires:

- Docker Desktop
- Docker Compose v2
- `make`
- Git
- Sufficient RAM and disk space

If a port conflict occurs, change the ports in `.env`. The ports most likely to conflict are:

- `AIRFLOW_WEB_PORT=8080`
- `FLINK_UI_PORT=8081`
- `SPARK_MASTER_UI_PORT=8082`
- `KAFKA_UI_PORT=8083`
- `SUPERSET_PORT=8088`
- `POSTGRES_AIRFLOW_PORT=15432`
- `POSTGRES_SOURCE_PORT=15433`

## Step 1: Prepare the Environment File

Before the first startup:

```powershell
Copy-Item .env.example .env
```

The `.env` file stores all local Docker Compose configuration:

- image names
- ports
- database names
- local user/password values
- Kafka topic name
- producer parameters
- Superset parameters

Do not commit `.env` to Git.

## Step 2: Validate the Compose Configuration

Before starting the platform:

```powershell
make config
```

This command runs:

```text
docker compose config
```

This checks that:

- `.env` values are read correctly.
- `docker-compose.yml` has valid syntax.
- volume, port, service, and environment mappings are valid.

If this reports an error, fix it before running `make up`.

## Step 3: Start the Platform

Primary start command:

```powershell
make up
```

This command builds and starts the following services:

- `postgres-airflow`
- `postgres-source`
- `kafka`
- `kafka-init`
- `kafka-ui`
- `minio`
- `minio-init`
- `nessie`
- `spark-master`
- `spark-worker`
- `clickhouse`
- `superset`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`
- `flink-jobmanager`
- `flink-taskmanager`

The first startup may take longer because images must be built and pulled.

## Step 4: Check Service Status

Container status:

```powershell
make ps
```

or:

```powershell
make status
```

This command runs `docker compose ps`.

Health check:

```powershell
make health
```

This command runs `scripts/health_check.py` inside the one-shot `platform-tools` container and checks the main services. The producer application image does not include health-check, ClickHouse, or Superset dependencies:

- Kafka
- PostgreSQL
- MinIO
- Nessie
- Spark
- ClickHouse
- Superset
- Flink
- Airflow

## Step 5: Get the URLs

To view all endpoints:

```powershell
make console
```

For browser URLs only:

```powershell
make urls
```

Primary URLs:

```text
Kafka UI:      http://localhost:8083
Flink UI:      http://localhost:8081
Spark UI:      http://localhost:8082
Airflow UI:    http://localhost:8080
ClickHouse:    http://localhost:8123
Superset:      http://localhost:8088
Nessie API:    http://localhost:19120/api/v2
MinIO API:     http://localhost:9000
MinIO Console: http://localhost:9001
```

Default local login:

```text
Airflow:  admin / admin
Superset: admin / admin
```

## Step 6: Run the Stream Pipeline

Stream pipeline:

```text
Producer -> Kafka -> Flink -> ClickHouse
```

```mermaid
sequenceDiagram
    participant P as Producer
    participant K as Kafka
    participant F as Flink
    participant C as ClickHouse

    P->>K: Publish delivery event
    K->>F: Flink consumes event
    F->>F: Parse JSON and validate schema
    F->>C: Write delivery_events
    F->>C: Write delivery_current_state
    F->>C: Write vehicle_current_state
```

Submit the Flink job first:

```powershell
make submit-flink-job
```

Then generate data:

```powershell
make produce
```

By default, `make produce` creates batch source data and publishes Kafka stream events.

To generate stream events only:

```powershell
make produce-stream
```

To verify the result in ClickHouse:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_events"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_current_state"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.vehicle_current_state"
```

## Step 7: Generate Batch Source Data

To seed the PostgreSQL source tables separately:

```powershell
make seed-batch-source
```

This command runs the generator application with `PRODUCER_MODE=batch`.

To inspect the PostgreSQL source tables:

```powershell
docker compose exec postgres-source psql -U postgres -d logistics_source -c "\dt"
```

Order sample:

```powershell
docker compose exec postgres-source psql -U postgres -d logistics_source -c "SELECT order_id, customer_region, service_level, priority, package_count, order_value FROM customer_orders LIMIT 10;"
```

For a detailed explanation of these tables:

```text
docs/postgres-source-tables.md
```

## Step 8: Test Iceberg Connectivity

To verify that Spark, Nessie, Iceberg, and MinIO work together:

```powershell
make spark-iceberg-test
```

Expected marker on success:

```text
ICEBERG_SMOKE_TEST_OK
```

This test verifies that:

- A Spark session opens.
- The Nessie catalog can be reached.
- Iceberg namespace/table operations work.
- The MinIO warehouse path is used.

## Step 9: Run the Batch KPI Job

Daily KPI job:

```powershell
make spark-daily-kpi
```

This command runs `spark-submit` inside the `spark-master` container. It is structured this way so the Spark job uses the Spark image classpath, `spark-defaults.conf`, and the application files under `/opt/deliveryflow/src/etl/apps/`.

```text
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/deliveryflow/src/etl/apps/daily_kpi_job.py
```

Batch flow:

```mermaid
sequenceDiagram
    participant C as ClickHouse
    participant S as Spark
    participant I as Iceberg
    participant N as Nessie
    participant M as MinIO
    participant BI as Superset

    S->>C: Read delivery.delivery_events
    S->>S: Calculate KPIs
    S->>I: Write bronze.raw_delivery_events
    S->>I: Write gold.daily_delivery_kpi
    I->>N: Commit table metadata
    I->>M: Store data and metadata files
    S->>C: Publish delivery.daily_delivery_kpi
    BI->>C: Read dashboard data
```

Expected marker on success:

```text
DAILY_KPI_JOB_OK
```

To verify KPI results:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM delivery.daily_delivery_kpi LIMIT 10"
```

## Step 9b: Run the Transportation Cost Batch Flow

Transportation Cost batch job:

```powershell
make run-batch DATASET=transportation-costs
```

For a specific business date:

```powershell
make run-batch DATASET=transportation-costs BATCH_DATE=2026-09-04
```

This runs the full Dashboard 6 batch ETL path:

```text
Synthetic Transportation Data
        |
        v
PostgreSQL Source
        |
        v
Airflow / Spark submit
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
ClickHouse Serving
        |
        v
Superset Dataset
        |
        v
Transportation Cost & Route Performance
```

The Spark job reads `public.transportation_costs`, validates cost data quality, writes `nessie.bronze.transportation_costs`, `nessie.silver.transportation_costs_enriched`, and `nessie.gold.daily_transportation_cost_kpi`, then publishes `delivery.daily_transportation_cost_kpi`.

To verify serving rows:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM delivery.v_transportation_cost_performance LIMIT 10"
```

Detailed runbook:

```text
docs/transportation-cost-flow.md
```

## Step 10: Data Sources for the Superset Dashboard

Superset URL:

```text
http://localhost:8088
```

ClickHouse connection URI:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

Import Superset objects as BI-as-code:

```powershell
make import-superset-assets
```

This command creates or updates database, dataset, chart, and dashboard objects through the Superset REST API using `configs/superset/deliveryflow_bi.yaml`.

This import means the dashboard does not need to be created manually in the Superset UI. The importer manages:

- `DeliveryFlow ClickHouse` database connection.
- Delivery operations datasets based on ClickHouse views.
- Transportation cost performance dataset based on `delivery.v_transportation_cost_performance`.
- Delivery operations statistical charts.
- Transportation cost management charts.
- The `DeliveryFlow Operations Dashboard` dashboard.
- The `Transportation Cost & Route Performance` dashboard.
- Adhoc metric formatting for chart metrics. For example, the `event_count` column is written as `SUM(event_count)` and `delay_rate` as `AVG(delay_rate)` in the chart.
- Datetime metadata for timeseries charts. In the `Hourly Delivery Event Volume` chart, `event_hour` is both the dataset temporal column and the chart `x_axis` / `granularity_sqla` value.

Prepared views for the dashboard:

- `delivery.v_delivery_status_overview`
- `delivery.v_delay_by_region`
- `delivery.v_vehicle_utilization`
- `delivery.v_warehouse_daily_kpi`
- `delivery.v_delivery_event_volume`
- `delivery.v_transportation_cost_performance`

To verify the views:

```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

Report and chart details:

```text
docs/superset-serving.md
```

## Step 11: End-to-End Smoke Test

Smoke test for the stream path:

```powershell
make test-e2e
```

This command:

- creates a synthetic event with the producer
- waits for a `delivery_events` row in ClickHouse
- waits for a `delivery_current_state` row
- confirms that the stream path works

Expected marker:

```text
STREAMING_E2E_OK
```

## Practical Make Command Usage

### `make help`

Shows the available Make commands in the project.

```powershell
make help
```

### `make config`

Validates the Docker Compose configuration. Run it after changing `.env` or `docker-compose.yml`.

```powershell
make config
```

### `make pull`

Pulls pinned upstream images. Useful during initial setup or when the image cache is outdated.

```powershell
make pull
```

### `make build`

Builds local images: Spark, Airflow, Flink, the minimal Producer, one-shot Platform Tooling, and Superset.

```powershell
make build
```

### `make up`

Starts the full local platform.

```powershell
make up
```

This command also starts the `producer-continuous` service with the core services. By default, that service sends 10 stream events to Kafka every 60 seconds.

### `make ps` and `make status`

Shows container status.

```powershell
make ps
make status
```

### `make health`

Checks readiness for the main services.

```powershell
make health
```

### `make console` and `make urls`

Shows service endpoints.

```powershell
make console
make urls
```

### `make submit-flink-job`

Submits the Flink streaming job. Run it before generating Kafka events.

```powershell
make submit-flink-job
```

### `make produce`

In the default generator mode, creates PostgreSQL batch source rows and publishes Kafka stream events.

```powershell
make produce
```

### `make produce-stream`

Generates Kafka stream events only.

```powershell
make produce-stream
```

### `make produce-continuous`

Starts the background `producer-continuous` service. It runs with `PRODUCER_MODE=stream` and `PRODUCER_CONTINUOUS=true`, and by default sends a batch of 10 delivery events to Kafka every 60 seconds.

```powershell
make produce-continuous
```

Change the interval in `.env`:

```text
PRODUCER_CONTINUOUS_INTERVAL_SECONDS=60
PRODUCER_EVENTS_PER_INTERVAL=10
```

Log-lara baxmaq:
View the logs:

```powershell
make logs-producer-continuous
```

To stop it:

```powershell
make stop-continuous-producer
```

### `make seed-batch-source`

Seeds only the PostgreSQL source tables.

```powershell
make seed-batch-source
```

### `make spark-iceberg-test`

Runs a smoke test for Spark, Iceberg, Nessie, and MinIO connectivity.

```powershell
make spark-iceberg-test
```

### `make spark-daily-kpi`

Runs the daily KPI batch job. Run it after ClickHouse contains `delivery_events` data.

```powershell
make spark-daily-kpi
```

### `make run-batch`

Runs a metadata-backed batch dataset job.

Default behavior runs the existing delivery daily KPI job:

```powershell
make run-batch
```

Transportation Cost batch flow:

```powershell
make run-batch DATASET=transportation-costs
```

Optional date-scoped rerun:

```powershell
make run-batch DATASET=transportation-costs BATCH_DATE=2026-09-04
```

This command writes the transportation cost Bronze, Silver, and Gold Iceberg tables and publishes the ClickHouse serving table used by Dashboard 6.

### `make import-superset-assets`

Creates or updates Superset objects from the repository YAML file.

```powershell
make import-superset-assets
```

This Makefile target first rebuilds the `superset-importer` image, then runs the Docker Compose service:

```text
docker compose build superset-importer
docker compose run --rm superset-importer
```

YAML source of truth:

```text
configs/superset/deliveryflow_bi.yaml
```

The import creates the `DeliveryFlow ClickHouse` database connection, delivery operations assets, and the `Transportation Cost & Route Performance` dashboard in Superset.

The importer also normalizes Superset chart parameters:

- Human-readable metric names stored in YAML are converted to Superset adhoc metric format.
- `SUM(...)` is used for count and sum columns.
- `AVG(...)` is used for columns with an `avg_` prefix or `_rate` suffix.
- Time columns such as `event_hour` and `business_date` are recorded as temporal columns in dataset metadata.
- `Hourly Delivery Event Volume` uses `x_axis: event_hour`, `granularity_sqla: event_hour`, and `time_grain_sqla: PT1H`.

This behavior prevents the following Superset errors:

```text
Metric 'event_count' does not exist
Metric 'delay_rate' does not exist
Datetime column not provided as part table configuration and is required by this type of chart
```

### `make airflow-dag-list`

Shows the DAG list in Airflow.

```powershell
make airflow-dag-list
```

### `make test-e2e`

Runs an end-to-end smoke test for the stream path.

The test runs in the `platform-tools` image; the producer application image contains only event-generation and PostgreSQL batch-seeding dependencies.

```powershell
make test-e2e
```

### Log Commands

To follow service logs:

```powershell
make logs
make logs-kafka
make logs-kafka-ui
make logs-flink
make logs-spark
make logs-airflow
make logs-clickhouse
make logs-superset
make logs-postgres
make logs-storage
make logs-nessie
```

### `make down`

Stops containers but keeps named volumes.

```powershell
make down
```

### `make clean`

Removes containers and orphan containers but keeps named volumes.

```powershell
make clean
```

### `make clean-keep-images`

Removes containers and orphan containers but keeps named volumes and Docker images.

```powershell
make clean-keep-images
```

### `make purge`

Performs destructive cleanup:

- container-ləri silir
- named volume-ları silir
- local project image-ləri silir
- orphan container-ləri silir
- Removes containers
- Removes named volumes
- Removes local project images
- Removes orphan containers

```powershell
make purge
```

Warning: `make purge` removes local data.

## Recommended Full Demo Sequence

For a demo from scratch:

```powershell
Copy-Item .env.example .env
make config
make up
make ps
make health
make submit-flink-job
make produce
make spark-iceberg-test
make spark-daily-kpi
make run-batch DATASET=transportation-costs
make import-superset-assets
make console
```

Then open these in a browser:

- Kafka UI: `http://localhost:8083`
- Flink UI: `http://localhost:8081`
- Spark UI: `http://localhost:8082`
- Airflow UI: `http://localhost:8080`
- Superset UI: `http://localhost:8088`

## Debug Workflow
## Troubleshooting Guide

If data does not reach ClickHouse:
### Issue 1: Events Do Not Reach ClickHouse

1. Check the Kafka logs:
**Problem:**
Events are produced, but `delivery.delivery_events` in ClickHouse remains empty.

**Possible Cause:**
The Flink streaming job is not running, or Kafka consumer connectivity failed.

**How to Check:**
Check Kafka and Flink job manager logs:
```powershell
make logs-kafka
make logs-flink
```

2. Check the Flink job logs:

Inspect ClickHouse row count:
```powershell
make logs-flink
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_events"
```

3. Generate a stream event:

**How to Fix:**
Submit the Flink streaming job and send test stream events:
```powershell
make submit-flink-job
make produce-stream
```

4. Check the ClickHouse row count:
**Expected Result:**
ClickHouse returns a positive row count in `delivery.delivery_events`.

### Issue 2: Fleet Telemetry Does Not Appear in ClickHouse

**Problem:**
Telemetry events for `APP=fleet` do not appear in `fleet.vehicle_telemetry_events`.

**Possible Cause:**
The `VehicleTelemetrySqlJob` Flink SQL application was not submitted.

**How to Check:**
Check Flink running jobs in the Flink Web UI (`http://localhost:8081`) or logs:
```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_events"
make logs-flink
```

If Superset views are missing:
**How to Fix:**
Submit the Fleet Flink SQL job and trigger fleet data generation:
```powershell
make submit-flink-job APP=fleet
make produce APP=fleet
```

**Expected Result:**
Rows appear in `fleet.vehicle_telemetry_events` and `fleet.vehicle_current_state`.

### Issue 3: Superset Views or Metrics Return Errors

**Problem:**
Superset dashboard charts display metric errors, missing datasets, or column type mismatches.

**Possible Cause:**
Superset metadata assets are out of sync with ClickHouse serving tables.

**How to Check:**
List existing serving tables in ClickHouse:
```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

If the dashboard exists but charts report metric or datetime errors:

**How to Fix:**
Re-import the BI-as-code configuration assets:
```powershell
make import-superset-assets
```
Then hard-refresh your browser tab (`Ctrl + F5`).

Then hard-refresh the dashboard page:
**Expected Result:**
All 5 delivery charts render without errors on the Superset dashboard (`http://localhost:8088`).

```text
Ctrl + F5
```
### Issue 4: Schema Mismatch from Stale Docker Volumes

If the new schema is missing, an old volume remains. If deleting local data is acceptable:
**Problem:**
ClickHouse or PostgreSQL container startup fails or retains outdated table definitions.

**Possible Cause:**
Named Docker volumes from previous runs contain old schema state.

**How to Check:**
Inspect volume definitions and container errors:
```powershell
docker compose ps
make logs-all
```

**How to Fix:**
Purge stale containers and volumes, then recreate the platform:
```powershell
make purge
make up
make health
```

**Expected Result:**
Clean initialization scripts execute on first boot, and all tables match current schema contracts.

## Additional Documentation

For deeper reading:

- `docs/application-workflow.md`: application, generator, stream, and batch workflow.
- `docs/transportation-cost-flow.md`: Dashboard 6 transportation cost batch flow.
- `docs/postgres-source-tables.md`: PostgreSQL source tables and inspection commands.
- `docs/superset-serving.md`: Superset dashboard strategy and five reports.
- `docs/architecture-decisions.md`: service choices and ADRs.
- `requirement.md`: overall requirements.
- `plan.md`: project plan.

## Important Notes

- Submit the Flink job before generating streaming events.
- ClickHouse must contain `delivery_events` data before running batch KPIs.
- ClickHouse is the approved data source for Superset.
- Kafka is an event transport layer, not an analytical database.
- Iceberg/Nessie/MinIO provide the analytical lakehouse layer.
- `make clean` does not remove data volumes.
- `make purge` removes data volumes.
