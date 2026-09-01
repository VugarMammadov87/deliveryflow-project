# DeliveryFlow Scalability and Multi-Application Architecture

This document answers the requirements in `DeliveryFlow_Scalability_and_Flink_SQL_Prompts.md` and records the target design used by the current repository. The goal is not to make the local Docker Compose lab prematurely distributed. The goal is to keep DeliveryFlow from growing into one custom Python file, one custom Spark job, one custom Airflow DAG, one Flink job, one Kafka topic, and one dashboard file per table.

The target rule is:

```text
More applications or tables should mostly mean:
more metadata
more contracts
more compute/storage capacity

It should not mostly mean:
more duplicated code
more copied DAGs
more hardcoded table names
more manual deployment steps
```

## Current Repository State

DeliveryFlow currently has two streaming applications:

```text
delivery
  Producer -> Kafka delivery-events -> DeliveryStreamingJob -> ClickHouse delivery.* -> Superset

fleet
  Producer APP=fleet -> Kafka vehicle-telemetry-events -> VehicleTelemetrySqlJob -> ClickHouse fleet.*
```

Important repository assets:

```text
configs/applications/delivery.yaml
configs/applications/fleet.yaml
configs/datasets/fleet/vehicle_telemetry.yaml
src/contracts/delivery/delivery_event_v1.schema.json
src/contracts/fleet/vehicle_telemetry_v1.schema.json
src/producers/
services/flink/src/main/java/local/deliveryflow/DeliveryStreamingJob.java
services/flink/src/main/java/local/deliveryflow/VehicleTelemetrySqlJob.java
services/clickhouse/init/001_delivery_schema.sql
configs/superset/deliveryflow_bi.yaml
```

The delivery application remains the default operator path:

```powershell
make submit-flink-job
make produce
make test-e2e
```

The fleet application is selected explicitly:

```powershell
make submit-flink-job APP=fleet
make produce APP=fleet
make test-e2e APP=fleet
```

## Scale Risks in the Current Baseline

### One Custom File Per Table

Current design: the project still has a small number of explicit jobs and DDL objects, which is acceptable for a local lab.

Scale problem: at 300-500 tables, table-specific scripts, DAGs, jobs, and dashboard definitions become the main source of failure. Every onboarding needs code review, deployment, and manual validation.

Recommended architecture: table behavior should be described in dataset metadata. Shared code should read metadata and execute common ingestion, validation, publishing, and observability patterns.

Trade-off: metadata-driven design adds a schema for metadata and validation work. It should be introduced first for repeatable ingestion and orchestration, not for complex business transformations.

When to apply: now for registries and simple ingestion patterns; later for generic CDC and backfill frameworks.

### One Flink Job Per Table

Current design: there is one delivery DataStream job and one fleet SQL/Table API job.

Scale problem: 500 always-running jobs would create excessive checkpoint, TaskManager, deployment, monitoring, and ownership overhead.

Recommended architecture: group streaming jobs by application, domain, or business capability. Use separate jobs when failure blast radius, state size, SLA, ownership, or deployment cadence are meaningfully different.

Trade-off: grouped jobs need careful routing and state design. Over-grouping creates a giant job; over-splitting creates operations overhead.

When to apply: keep delivery and fleet separate. For future domains, prefer one job per application capability first, then split only when state or SLA demands it.

### One Spark Job Per Table

Current design: `src/etl/apps/daily_kpi_job.py` is a specific KPI batch job.

Scale problem: hundreds of table-specific Spark apps duplicate source reading, watermark handling, Iceberg writes, ClickHouse publication, retry, and backfill logic.

Recommended architecture: add a metadata-driven Spark ingestion framework for common full, incremental, CDC, merge/upsert, and publish patterns. Keep complex business transformations as code modules referenced by metadata.

Trade-off: the first generic Spark runner takes more design effort than another script, but prevents long-term copy/paste.

When to apply: before the third or fourth similar ingestion job is added.

### One Airflow DAG Per Table

Current design: `dags/delivery_daily_kpi.py` is a hand-written workflow for the current KPI flow.

Scale problem: hundreds of DAG files make scheduling, retries, dependency management, and ownership hard to reason about.

Recommended architecture: use a DAG factory or metadata-driven DAGs by application/domain. Use dynamic task mapping for table lists and Airflow pools for source-system, application, Spark, and ClickHouse limits.

Trade-off: DAG generation must be deterministic and validated in CI so Airflow parse failures are caught early.

When to apply: when the platform adds multiple batch datasets with similar schedules.

### One Kafka Topic Per Table

Current design: `delivery-events` and `vehicle-telemetry-events` are domain event topics, not table topics.

Scale problem: one topic per table causes topic and partition explosion, broker metadata overhead, retention sprawl, and noisy consumer group management.

Recommended architecture: use topics by event domain, entity, event family, or business capability. Do not map every physical table to a topic.

Trade-off: shared domain topics require clear schemas and routing fields. Too much sharing weakens isolation.

When to apply: keep the current two topics; add new topics only when ownership, retention, ordering, or failure isolation requires it.

### One Superset YAML for Everything

Current design: `configs/superset/deliveryflow_bi.yaml` owns the delivery dashboard.

Scale problem: 100+ charts in one file becomes hard to review, import, and delegate to application teams.

Recommended architecture: keep the current delivery file, then introduce `configs/superset/<app>/` when another BI domain is promoted from serving tables to dashboards.

Trade-off: modular BI assets need importer discovery and ordering logic.

When to apply: before adding fleet BI dashboards or a third BI domain.

## Control Plane and Data Plane

DeliveryFlow should use a lightweight control-plane model, not a separate control-plane service yet.

```text
CONTROL PLANE
  Git-versioned metadata
  Application registry
  Dataset registry
  Event contracts
  Quality rules
  Ownership and SLA
  Scheduling and deployment metadata

DATA PLANE
  Kafka
  Flink
  Spark
  Airflow
  Iceberg
  Nessie
  MinIO
  ClickHouse
  Superset
```

Current local implementation:

```text
configs/applications/*.yaml       application registry
configs/datasets/<domain>/*.yaml  dataset registry
src/contracts/<domain>/*.json     versioned contracts
Makefile APP=<name>               runtime application selector
Docker Compose                    local data plane
```

Recommended metadata storage:

- Git/YAML/JSON is the source of truth for local development, CI review, contracts, ownership, schedules, and static configuration.
- PostgreSQL should be added for runtime control-plane state only when operators need UI/API-driven onboarding, dynamic status, locks, audit trails, or self-service changes.
- A hybrid model is the production target: Git remains the reviewed desired state; PostgreSQL can hold runtime state, execution history, and resolved deployment records.

Runtime should read metadata through small adapters. Do not make every Java, Python, and DAG component parse ad hoc YAML differently. The future path is a shared metadata loader with validation tests.

## Target Metadata Model

A production-grade dataset record should cover:

```yaml
dataset_id: fleet.vehicle_telemetry
application: fleet
domain: fleet-operations
description: Vehicle telemetry stream used for fleet health monitoring.

source:
  type: kafka
  topic: vehicle-telemetry-events
  key: vehicle_id

contract: src/contracts/fleet/vehicle_telemetry_v1.schema.json

ingestion:
  mode: streaming
  load_type: append
  primary_key:
    - event_id
  watermark_column: event_timestamp
  late_arrival_tolerance_seconds: 10

target:
  layer: serving
  system: clickhouse
  database: fleet
  tables:
    - vehicle_telemetry_events
    - vehicle_current_state
    - vehicle_health_alerts
    - vehicle_metrics_5m

schedule:
  type: continuous

quality:
  not_null:
    - event_id
    - event_timestamp
    - vehicle_id
  accepted_values:
    engine_status:
      - RUNNING
      - IDLE
      - OFF

owner:
  team: logistics-data-platform

sla:
  freshness_minutes: 5

retention:
  raw_days: 30

criticality: medium
bi:
  exposed: false
  future_scope: configs/superset/fleet
```

For batch datasets, the same model should add:

```yaml
source:
  type: postgres
  connection: logistics_source
  schema: public
  table: customer_orders

ingestion:
  mode: incremental
  load_type: merge
  primary_key:
    - order_id
  watermark_column: updated_at

target:
  layer: bronze
  system: iceberg
  namespace: logistics
  table: customer_orders

publish:
  clickhouse:
    enabled: true
    database: delivery
    table: customer_orders_serving
```

## Repository Architecture

The current repository should evolve conservatively:

```text
configs/
  applications/
  datasets/
  superset/

src/
  contracts/
  producers/
  etl/
    apps/
    framework/          future shared Spark readers/writers/loaders

services/
  flink/
  spark/
  clickhouse/
  producer/
  tooling/

dags/
  generated or factory DAGs in a later phase
```

Do not move everything into a new `platform/` and `apps/` hierarchy at once. The current repo already has service-oriented folders. The next scalable improvement is adding shared `src/etl/framework/` modules and metadata validators while keeping existing paths stable.

Shared platform logic:

- Kafka publishing transport.
- Producer config parsing.
- Metadata loading and validation.
- Spark readers and writers.
- Iceberg write helpers.
- ClickHouse publish helpers.
- E2E and health-check utilities.
- Superset importer behavior.

Application-specific logic:

- Delivery event schema and state rules.
- Fleet telemetry schema and health rules.
- Domain SQL transformations.
- Domain serving tables.
- Domain BI dashboards.

## Application Registry

Application metadata belongs under `configs/applications/`.

Current examples:

```text
configs/applications/delivery.yaml
configs/applications/fleet.yaml
```

An application record should answer:

- who owns it;
- whether streaming, batch, serving, and BI are enabled;
- which Flink job or Spark workflow belongs to it;
- which Kafka topics and ClickHouse schemas it owns;
- which runtime thresholds are stable business settings.

Application isolation standard:

- default local app remains `delivery`;
- `APP=fleet` selects the fleet runtime path;
- each app gets its own event contracts and serving schema;
- separate Flink jobs are used when application failure isolation is required;
- shared Docker Compose infrastructure is reused in the local lab.

## Dataset Registry

Dataset metadata belongs under `configs/datasets/<domain>/`.

The dataset registry should become the onboarding surface for new tables. A new standard table should not require a new script if it is a full load, incremental load, CDC mirror, Iceberg write, ClickHouse publish, or simple quality-check pattern already supported by the platform.

Dataset metadata should include:

- `dataset_id`;
- application and domain;
- source system, schema, table, topic, or path;
- event contract or table schema;
- load mode: full, incremental, CDC, streaming, backfill;
- primary key;
- watermark;
- partitioning;
- target layer and table;
- schedule;
- SLA;
- owner;
- quality rules;
- retention;
- criticality;
- BI exposure.

## Kafka Architecture

Recommended naming convention:

```text
<environment>.<domain>.<entity>.<event-family>
```

Local topics may stay shorter for readability:

```text
delivery-events
vehicle-telemetry-events
```

Production examples:

```text
prod.delivery.delivery.lifecycle
prod.fleet.vehicle.telemetry
prod.warehouse.inventory.movement
```

Topic strategy comparison:

- Topic per table: avoid by default; it causes topic and partition explosion.
- Topic per entity: good when one entity has clear ownership and ordering needs.
- Topic per domain: good for related event families with the same retention and consumers.
- Topic per event family: best default for DeliveryFlow because it balances isolation and manageability.
- Shared event topic: useful only for low-volume generic audit streams; avoid for core business events.

Local topology:

```text
1 KRaft broker
replication factor 1
small fixed partition count
```

Production minimum realistic topology:

```text
3+ brokers
replication factor 3
rack/zone awareness when available
separate monitoring for broker disk, ISR, request latency, consumer lag, partition skew
```

Scale out Kafka when broker disk, network throughput, request latency, under-replicated partitions, consumer lag, or partition hot spots show sustained pressure. Do not scale merely because table count increased.

## Schema Registry Decision

Current repository uses JSON plus `schema_version = 1` and Git-versioned JSON Schema files.

Options:

- JSON Schema in Git: lowest operational complexity; good for this local platform and early multi-application work.
- Kafka Schema Registry with Avro: strong compatibility enforcement and compact payloads; useful when many teams produce/consume events.
- Kafka Schema Registry with Protobuf: good for strongly typed service ecosystems and long-lived APIs.
- Hybrid: keep JSON Schema in Git now; introduce Schema Registry for high-volume or multi-team domains later.

Recommendation: do not add Schema Registry only to look enterprise. Add it when compatibility enforcement, producer/consumer independence, CI integration, and multi-team schema governance become operationally necessary.

## Flink Architecture

Current state:

```text
DeliveryStreamingJob       Java DataStream API
VehicleTelemetrySqlJob     Java Flink SQL / Table API
```

Grouping strategy:

- One giant job: avoid; upgrades and failures affect every domain.
- One job per table: avoid; too much runtime overhead.
- One job per application: good default while domains are small.
- One job per domain: good when one application owns independent subdomains.
- One job per business capability: best once state size, SLA, or ownership differs inside an application.

Fleet uses Flink SQL because telemetry classification and window metrics are naturally expressed in SQL:

```text
Kafka source table
  -> valid telemetry filter
  -> raw history
  -> latest state projection
  -> threshold alert classification
  -> five-minute event-time metrics
  -> ClickHouse fleet.*
```

The 10-second watermark is intentionally small because synthetic local telemetry is generated in-order or near-order. In production, late-arrival tolerance should be based on device buffering, mobile network delay, and observed event-time skew.

Generic streaming pieces:

- Kafka source configuration.
- Contract/version validation.
- Logging and metrics.
- ClickHouse connectivity.
- Common sink error handling.

Fleet-specific pieces:

- telemetry schema;
- threshold rules;
- vehicle state projection;
- alert classification;
- fleet metric SQL.

## Spark and Batch Architecture

The current `daily_kpi_job.py` is acceptable for one business KPI flow. It should not become the pattern for hundreds of simple table loads.

Target framework:

```text
src/etl/framework/
  metadata/
  readers/
  writers/
  loaders/
  quality/
  publishing/
```

Reusable components should include:

- `PostgresReader`;
- `ClickHouseReader`;
- `IcebergWriter`;
- `ClickHouseWriter`;
- `FullLoader`;
- `IncrementalLoader`;
- `MergeLoader`;
- `QualityValidator`;
- backfill and reprocessing helpers.

New tables should be metadata-only when they are standard source-to-target movement. New code is still appropriate for complex business transformations, custom algorithms, optimization logic, or domain-specific joins.

## Airflow Architecture

Airflow should orchestrate Spark and operational workflows; it should not replace Spark or Flink.

For 300-500 tables, use:

- DAG factory for generated application/domain DAGs;
- dynamic task mapping for dataset lists;
- one DAG per application/domain where schedules align;
- dataset-aware scheduling when upstream/downstream datasets matter;
- pools for source systems, applications, Spark concurrency, and ClickHouse publish limits;
- deterministic DAG generation tests in CI.

Dependency metadata should model dataset edges instead of hardcoded Python chains:

```text
bronze.orders
  -> silver.orders
  -> gold.sales
  -> clickhouse.sales_serving
```

Airflow should compile this graph into tasks and task groups. Cycles, missing owners, missing SLAs, and unknown datasets should fail validation before deployment.

## Iceberg, MinIO, and Nessie

Iceberg can handle hundreds of tables. The real risks are metadata growth, many small files, snapshot growth, manifest bloat, and missing maintenance.

Namespace standard:

```text
<application>.<layer>.<dataset>

delivery.bronze.raw_delivery_events
delivery.gold.daily_delivery_kpi
fleet.bronze.vehicle_telemetry_events
```

Partitioning should be chosen by query and write patterns, not copied blindly. Time-based event tables usually start with day/month partitions; high-cardinality ids should not become top-level partitions.

Required maintenance:

- compact small files;
- expire old snapshots;
- remove orphan files;
- monitor manifest growth;
- document schema and partition evolution;
- validate replay and backfill idempotency.

Object storage should not use one bucket per table. Keep a small number of buckets and let the catalog manage table locations:

```text
s3://delivery-lakehouse/warehouse/<namespace>/<table>/
```

Nessie should be used as the catalog and for deliberate promotion/release workflows. Do not create a branch for every ETL run. Use branches/tags when testing data promotion, reproducing releases, or isolating larger backfills.

## PostgreSQL Separation

Current local setup:

```text
postgres-airflow
  airflow_metadata

postgres-source
  logistics_source
  nessie_metadata
```

This is acceptable for local Docker Compose. Production should separate failure domains and ownership:

- Airflow metadata DB.
- Nessie metadata DB.
- Control-plane metadata DB, if introduced.
- Business source PostgreSQL systems.

The local lab should not add more PostgreSQL containers until a concrete runtime need exists.

## ClickHouse Architecture

ClickHouse currently serves:

- real-time operational tables;
- BI/reporting views;
- Spark source reads for KPI jobs;
- Spark-published KPI tables.

Scale risks:

- concurrent streaming and batch writes;
- BI scans competing with ingestion;
- background merges;
- TTL and mutation load;
- wide table scans;
- materialized view complexity;
- query memory pressure.

Local standard:

```text
delivery.*   delivery operations and dashboard serving
fleet.*      fleet telemetry serving
```

Production standard:

- keep application/domain schemas;
- separate operational and BI views logically;
- use the same cluster until workload metrics justify separation;
- split clusters only when ingestion, BI concurrency, memory pressure, or ownership requires it;
- use `MergeTree` for append history;
- use `ReplacingMergeTree` for latest-state tables with explicit version columns;
- define TTLs based on retention requirements.

## Superset Architecture

Superset must read approved ClickHouse serving tables and views. It should not read Kafka, raw object storage, or operational source PostgreSQL tables for dashboards.

Current delivery BI is managed by:

```text
configs/superset/deliveryflow_bi.yaml
scripts/import_superset_assets.py
make import-superset-assets
```

Fleet tables exist but `bi.superset` is currently false in `configs/applications/fleet.yaml`. When fleet BI is approved, add modular BI assets under:

```text
configs/superset/fleet/
```

Future importer work should discover per-app BI files instead of forcing every dashboard into one YAML.

## Observability and Operations

At this scale, metadata must also drive operational checks:

- Kafka topic exists and lag is acceptable.
- Flink job is running with expected name.
- Checkpoint age and failures are visible.
- ClickHouse table row counts and freshness are tracked.
- Airflow DAG parse, schedule, and task failures are visible.
- Spark jobs emit row counts and watermark summaries.
- Iceberg maintenance status is tracked.
- Superset import status is reproducible.

Local health checks can stay simple, but the metadata model should provide enough information to generate checks later.

## Migration Roadmap

### Phase 1 - Current Multi-Application Foundation

Status: implemented in this repository.

Delivered:

- application metadata in `configs/applications/`;
- fleet dataset metadata in `configs/datasets/fleet/`;
- versioned delivery and fleet contracts;
- independent fleet Kafka topic;
- independent Java Flink SQL/Table API job;
- ClickHouse `fleet.*` serving tables;
- parameterized `APP=fleet` Makefile workflow;
- focused producer and smoke-test coverage.

### Phase 2 - Metadata Validation

Add CI tests that validate every application and dataset file against a repository metadata schema. Validate owners, SLAs, contracts, target tables, topic names, and unsupported load modes.

### Phase 3 - Generic Batch Ingestion Runner

Introduce `src/etl/framework/` for common full and incremental PostgreSQL/ClickHouse to Iceberg flows. Keep `daily_kpi_job.py` as a domain KPI job until similar jobs justify migration.

### Phase 4 - Airflow DAG Factory

Generate deterministic DAGs or task groups from dataset metadata. Add parse tests and pool/concurrency controls before onboarding many tables.

### Phase 5 - Modular BI Import

Split Superset assets by application when a second dashboard domain is approved. Extend the importer to discover `configs/superset/<app>/`.

### Phase 6 - CDC and Schema Governance

Add CDC and Schema Registry only after the platform has enough multi-team or compatibility pressure to justify the operational cost.

### Phase 7 - Production Scale-Out

Scale Kafka, Flink, Spark, ClickHouse, PostgreSQL, object storage, and Airflow based on measured bottlenecks, not table count alone.

## Definition of Done Mapping

The prompt requirements map to the repository as follows:

- Fleet contract exists: `src/contracts/fleet/vehicle_telemetry_v1.schema.json`.
- Synthetic producer creates telemetry events: `src/producers/events.py`.
- Fleet events publish to a separate topic: `vehicle-telemetry-events`.
- Java Flink SQL consumes the topic: `VehicleTelemetrySqlJob`.
- Event time and watermark are configured in the SQL source DDL.
- SQL transformations create raw, current state, health alerts, and metrics.
- ClickHouse has `fleet.vehicle_telemetry_events`.
- ClickHouse has `fleet.vehicle_current_state`.
- ClickHouse has `fleet.vehicle_health_alerts`.
- ClickHouse has `fleet.vehicle_metrics_5m`.
- Delivery remains the default application.
- Fleet is selected with `APP=fleet`.
- The structure supports a third application through metadata, contracts, target DDL, and only the distinct business processing code that is genuinely needed.
