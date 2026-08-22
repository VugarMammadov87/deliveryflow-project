# Codex Implementation Plan
## Real-Time Delivery Monitoring and Daily Transportation Optimization

> **Primary source of truth:** `requirement.md`
>
> This `plan.md` defines the approved implementation sequence for Codex. It does not override business, architecture, engineering, security, or approval rules in `requirement.md`. If there is any conflict, `requirement.md` wins.

---

# 0. Current Multi-Application Update

The scalability prompt `DeliveryFlow_Scalability_and_Flink_SQL_Prompts.md` has been applied as an implementation phase.

Completed changes:

- Add control-plane style metadata folders: `configs/applications/` and `configs/datasets/`.
- Keep the delivery application as the default flow.
- Add `fleet` as a second stream application.
- Add versioned fleet JSON contract under `src/contracts/fleet/`.
- Add Java Flink SQL/Table API job for vehicle telemetry.
- Add ClickHouse `fleet` database tables for raw telemetry, latest state, alerts, and five-minute metrics.
- Extend Makefile command routing with `APP=fleet`.
- Extend health and smoke-test scripts so both delivery and fleet topics/paths are validated.

Operational commands:

```powershell
make submit-flink-job
make produce
make test-e2e

make submit-flink-job APP=fleet
make produce APP=fleet
make test-e2e APP=fleet
```

This phase directly supports the 300-500 table target by moving future application-specific details into metadata and contracts instead of scattering them across unrelated scripts.

---

# 1. Purpose

This plan translates `requirement.md` into an incremental, approval-gated implementation roadmap for the local data platform.

The target platform combines:

- Real-time logistics event generation.
- Apache Kafka for event transport.
- Apache Flink for real-time processing.
- ClickHouse for low-latency operational and analytical serving.
- S3-compatible object storage for the Data Lake.
- Apache Iceberg for Data Lake analytical tables.
- Parquet as the underlying Iceberg data-file format.
- Nessie as the Iceberg catalog.
- Apache Spark for distributed batch computation.
- Apache Airflow with `LocalExecutor` for batch orchestration.
- PostgreSQL for Airflow metadata and approved relational/reference workloads.
- Superset for reporting and analytics.
- Docker Compose for local platform orchestration.
- Makefile for developer operations.

The implementation must remain understandable and practical for local development while preserving production-grade engineering principles.

---

# 2. Final Architecture Decisions

The following decisions are already approved and must **not** be reopened unless the user explicitly requests a change.

```text
Streaming serialization / event contracts -> JSON
Schema evolution                        -> Versioned JSON contracts
Data Lake data-file format              -> Parquet
Data Lake table format                  -> Apache Iceberg
Iceberg catalog                         -> Nessie
Airflow executor                        -> LocalExecutor
BI platform                             -> Superset
Relational / Airflow metadata database  -> PostgreSQL
Operational / analytical serving        -> ClickHouse
Batch compute                           -> Apache Spark
Streaming compute                       -> Apache Flink
Streaming backbone                      -> Apache Kafka
```

Important boundaries:

```text
Kafka      = streaming event backbone
Flink      = real-time stream processing
ClickHouse = low-latency operational and analytical serving
Data Lake  = replayable historical/raw storage
Parquet    = physical Iceberg data files
Iceberg    = Data Lake table format
Nessie     = Iceberg catalog
Spark      = distributed batch compute
Airflow    = workflow orchestration/control plane
PostgreSQL = Airflow metadata + explicitly approved relational/reference workloads
Superset   = BI/reporting layer
```

Airflow must not replace Spark or Flink.

ClickHouse must not replace the replayable Data Lake.

Nessie must not replace object storage.

PostgreSQL business/reference workloads must remain isolated from Airflow metadata by clear database/schema/role boundaries.

---

# 3. Target Logical Architecture

```text
                    Orders / Warehouses / Vehicles
                               |
                               v
                     Transportation Planning
                               |
                               v
                           Deliveries
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
          Real-Time Path               Historical Path
                 |                           |
                 v                           v
               Kafka                  S3-Compatible Storage
                 |                           |
                 v                           v
               Flink                    Iceberg Tables
                 |                           |
                 v                         Nessie
            ClickHouse                       |
                 |                           v
                 |                         Spark
                 |                           ^
                 |                           |
                 |                        Airflow
                 |                    LocalExecutor
                 |                           |
                 +-------------+-------------+
                               |
                               v
                            Superset
```

PostgreSQL supports Airflow metadata and explicitly approved relational/reference workloads.

---

# 4. Mandatory Codex Execution Protocol

Codex must perform only **one meaningful implementation action at a time**.

Before every meaningful action, Codex must provide:

```text
Current state:
...

Recommended next step:
...

Why:
...

Files/services affected:
...

Risk:
Low / Medium / High

Command:
<exact command, when required>

Impact:
Read-only / Non-destructive / Changes files / Starts services / Destructive

Expected result:
...

Approval required:
Do you want me to proceed with this exact step?
```

Then Codex must stop.

Approval applies only to the immediately preceding action.

The following responses:

```text
yes
continue
proceed
ok
do it
```

approve only that exact action.

They do not authorize later actions.

After an approved action:

1. Report exactly what changed.
2. Report unexpected findings.
3. Do not silently fix unrelated issues.
4. Do not automatically perform validation.
5. If validation is required, propose the validation as the next separate action.
6. Ask for approval.
7. Stop.

If an operation fails:

1. Stop.
2. Show the relevant error.
3. Explain the likely cause.
4. Recommend exactly one fix.
5. Explain affected files/services.
6. Ask for approval.
7. Do not automatically retry.

Destructive operations always require immediate explicit confirmation.

---

# 5. Repository Discovery

## Phase 0 - Read Requirements and Inspect Repository

### Objective

Understand the repository before creating or modifying anything.

### Actions

1. Read the complete `requirement.md`.
2. Read the complete `plan.md`.
3. Inspect the repository root.
4. Identify existing:
   - Docker files.
   - Environment files.
   - Makefiles.
   - Python packaging files.
   - Service directories.
   - Configuration files.
   - Documentation.
   - Tests.
5. Report the current state.
6. Recommend exactly one next step.

### Initial commands

Typical read-only inspection commands may include:

```bash
pwd
ls -la
find . -maxdepth 2 -type f | sort
find . -maxdepth 2 -type d | sort
```

Each command still requires approval according to `requirement.md`.

### Exit Criteria

- Existing repository structure is understood.
- No existing file has been overwritten.
- Potential conflicts are identified.
- The next action is based on the real repository state.

---

# 6. Remaining Architecture Decisions

Only unresolved decisions should be discussed.

## Phase 1 - Close Remaining ADRs

### ADR-001 - Kafka Local Deployment

Determine:

- Kafka distribution/image.
- KRaft configuration.
- Broker count.
- Controller configuration.
- Internal listener.
- External listener.
- Local replication factor.
- Retention policy.
- Resource limits.
- Health-check strategy.

Keep the local environment simple unless complexity solves a real requirement.

### ADR-002 - Kafka Topic Strategy

Compare:

```text
Single event topic
vs
Domain-specific topics
```

Potential domains:

```text
delivery-events
vehicle-location-events
shipment-status-events
warehouse-events
transportation-plan-events
```

Determine for every approved topic:

- Message key.
- Partition key.
- Partition count.
- Ordering requirement.
- Retention.
- Consumer groups.
- Replay behavior.
- Duplicate handling.
- Delivery semantics.
- Dead-letter handling where justified.

### ADR-003 - Flink Implementation API

Compare:

```text
PyFlink
vs
Java DataStream API
vs
Table API / Flink SQL
```

Evaluate:

- Stateful processing.
- Event-time support.
- Kafka connector maturity.
- ClickHouse sink integration.
- Testing.
- Operational maintainability.
- Developer productivity.
- Local development complexity.

### ADR-004 - S3-Compatible Object Storage

Compare the approved local candidates such as:

```text
MinIO
vs
RustFS
vs
another justified S3-compatible implementation
```

Evaluate:

- S3 API compatibility.
- Spark compatibility.
- Iceberg compatibility.
- Local resource consumption.
- Persistence.
- Operational simplicity.

### ADR-005 - ClickHouse Physical Design

Determine:

- Event-history tables.
- Current-state tables.
- Table engines.
- `ORDER BY` keys.
- Partitioning.
- Primary-key semantics.
- TTL.
- Retention.
- Insert batching.
- Deduplication.
- Version columns.
- Materialized views where justified.
- Flink-to-ClickHouse delivery semantics.
- Spark-to-ClickHouse publication semantics where used.

### ADR-006 - PostgreSQL Isolation

Determine:

- Airflow metadata database.
- Business/reference database if required.
- Roles.
- Schemas.
- Permissions.
- Connection limits.
- Persistence.
- Credential handling.

### ADR-007 - Iceberg Maintenance Strategy

Determine:

- Warehouse path.
- Namespaces.
- Partition strategy.
- Sort strategy where useful.
- Snapshot retention.
- Snapshot expiration.
- Orphan-file cleanup.
- Compaction strategy.
- Schema evolution rules.
- Partition evolution rules.

### ADR-008 - Nessie Strategy

Determine:

- Nessie version.
- Catalog endpoint.
- Namespace strategy.
- Warehouse configuration.
- Local branch/reference strategy.
- Persistence.
- Health check.
- Recovery behavior.

### ADR-009 - Superset Serving Strategy

Determine:

- Superset source.
- Approved ClickHouse connectivity approach.
- Import vs DirectQuery where supported and justified.
- Semantic-model boundaries.
- Operational vs management datasets.
- Refresh strategy.

### Exit Criteria

All decisions required before infrastructure creation are documented and explicitly approved.

---

# 7. Repository Foundation

## Phase 2 - Create Repository Foundation Incrementally

### Objective

Create only the minimum foundation required for the next approved service.

### Candidate Structure

```text
project-root/
â”œâ”€â”€ requirement.md
â”œâ”€â”€ plan.md
â”œâ”€â”€ .env
â”œâ”€â”€ .env.example
â”œâ”€â”€ .gitignore
â”œâ”€â”€ Makefile
â”œâ”€â”€ docker-compose.yml
â”œâ”€â”€ docker-compose.local.yml
â”‚
â”œâ”€â”€ services/
â”‚   â”œâ”€â”€ kafka/
â”‚   â”œâ”€â”€ flink/
â”‚   â”œâ”€â”€ spark/
â”‚   â”œâ”€â”€ airflow/
â”‚   â”œâ”€â”€ clickhouse/
â”‚   â”œâ”€â”€ object-storage/
â”‚   â”œâ”€â”€ nessie/
â”‚   â””â”€â”€ postgres/
â”‚
â”œâ”€â”€ dags/
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ producers/
â”‚   â”œâ”€â”€ streaming/
â”‚   â””â”€â”€ batch/
â”œâ”€â”€ configs/
â”œâ”€â”€ scripts/
â”œâ”€â”€ tests/
â”œâ”€â”€ docs/
â””â”€â”€ data/
```

This is a target structure, not authorization to create all directories immediately.

### Recommended Order

1. Inspect `.gitignore` if it exists.
2. Create/update `.gitignore`.
3. Create `.env.example`.
4. Create the initial Docker Compose file.
5. Create the Makefile foundation.
6. Create service directories only when each service is approved.

### Exit Criteria

- Repository conventions are established.
- No unused structure is created.
- No environment-specific secrets are committed.

---

# 8. Environment Configuration

## Phase 3 - Centralize Configuration

### Objective

Move environment-specific configuration outside source code.

### Candidate Variables

```text
COMPOSE_PROJECT_NAME

KAFKA_IMAGE
KAFKA_BROKER_PORT
KAFKA_CONTROLLER_PORT
KAFKA_CLUSTER_ID
KAFKA_INTERNAL_BOOTSTRAP_SERVERS
KAFKA_EXTERNAL_BOOTSTRAP_SERVERS

FLINK_IMAGE
FLINK_JOBMANAGER_PORT
FLINK_UI_PORT

SPARK_IMAGE
SPARK_MASTER_PORT
SPARK_MASTER_UI_PORT
SPARK_WORKER_MEMORY
SPARK_WORKER_CORES

AIRFLOW_IMAGE
AIRFLOW_WEB_PORT
AIRFLOW_EXECUTOR
AIRFLOW_DB_NAME

CLICKHOUSE_IMAGE
CLICKHOUSE_HTTP_PORT
CLICKHOUSE_NATIVE_PORT
CLICKHOUSE_DATABASE

S3_ENDPOINT
S3_ACCESS_KEY
S3_SECRET_KEY
S3_BUCKET
S3_REGION

NESSIE_IMAGE
NESSIE_PORT

POSTGRES_IMAGE
POSTGRES_PORT
POSTGRES_DATABASE
POSTGRES_USER
POSTGRES_PASSWORD

NETWORK_SUBNET
```

Approved Airflow executor:

```text
AIRFLOW_EXECUTOR=LocalExecutor
```

Do not create variables that are not used.

### Exit Criteria

- No production secrets are present.
- Local credentials are clearly marked development-only.
- Ports, images, hosts, credentials, endpoints, bucket names, and database names are not unnecessarily hardcoded.

---

# 9. Docker Networking and Persistence

## Phase 4 - Establish Shared Infrastructure

### Objective

Create the Docker networking and persistence foundation.

### Shared Network

Candidate:

```text
delivery-platform-net
```

### Internal Service Names

```text
kafka:9092
clickhouse:8123
postgres:5432
airflow-webserver:8080
nessie:19120
spark-master:7077
```

Never use `localhost` for container-to-container communication.

### Named Volumes

Potential persistent data:

```text
Kafka data
ClickHouse data
Object Storage data
PostgreSQL data
Airflow metadata through PostgreSQL
Airflow logs where justified
Nessie metadata
```

### Rules

- `make down` must not delete persistent volumes.
- Destructive cleanup must be separate.
- Do not expose internal ports unless host access is required.

### Exit Criteria

- Shared network exists.
- Persistent volumes are declared.
- Internal/external endpoint conventions are clear.

---

# 10. PostgreSQL Foundation

## Phase 5 - Configure PostgreSQL

### Objective

Provide a durable relational store for Airflow metadata and approved relational/reference workloads.

### Recommended Sequence

```text
PostgreSQL Compose definition
        â†“
Named persistent volume
        â†“
Readiness health check
        â†“
Airflow metadata database
        â†“
Airflow role/user
        â†“
Optional isolated business/reference database
```

### Isolation Principle

```text
PostgreSQL Instance
â”œâ”€â”€ airflow_metadata
â”‚   â””â”€â”€ airflow role
â””â”€â”€ logistics_reference
    â””â”€â”€ application role
```

The exact second database is created only if required.

### Validation

Potential checks:

- PostgreSQL readiness.
- Database connectivity.
- Role isolation.
- Persistent restart behavior.

### Exit Criteria

- PostgreSQL is healthy.
- Airflow metadata storage is isolated.
- Credentials are environment-driven.
- Persistence survives normal restart.

---

# 11. Kafka Foundation

## Phase 6 - Configure Kafka

### Objective

Establish the event backbone.

### Recommended Sequence

```text
Kafka Compose service
        â†“
KRaft configuration
        â†“
Internal listener
        â†“
External listener
        â†“
Persistent volume
        â†“
Readiness health check
        â†“
Configuration validation
        â†“
Service startup
        â†“
Topic provisioning
        â†“
Producer/consumer smoke test
```

Every arrow may represent a separate approval-gated action.

### Validation

Potential validations:

```bash
docker compose config
docker compose up -d kafka
docker compose ps
```

Do not execute these automatically.

### Exit Criteria

- Kafka is healthy.
- Internal and external connectivity work.
- Approved topics exist.
- A test producer/consumer exchange succeeds.

---

# 12. Object Storage Foundation

## Phase 7 - Configure S3-Compatible Storage

### Objective

Provide the physical storage layer for Iceberg data and metadata files.

### Responsibilities

Object storage will contain:

```text
Parquet data files
Iceberg metadata files
Iceberg manifests
Iceberg manifest lists
```

Nessie remains the catalog.

### Recommended Sequence

```text
Object-storage service
        â†“
Persistent volume
        â†“
Health check
        â†“
Development credentials
        â†“
Warehouse bucket
        â†“
Spark S3 connectivity test
```

### Exit Criteria

- Storage is healthy.
- S3 API is reachable from containers.
- Warehouse bucket exists.
- Spark-compatible credentials/endpoints are available.

---

# 13. Nessie Foundation

## Phase 8 - Configure Nessie

### Objective

Provide the approved Iceberg catalog.

### Architecture

```text
Spark
  |
  v
Nessie Catalog
  |
  v
Iceberg metadata references
  |
  v
S3-Compatible Object Storage
```

### Recommended Sequence

```text
Nessie service
    â†“
Version compatibility validation
    â†“
Catalog endpoint
    â†“
Persistence configuration
    â†“
Health check
    â†“
Warehouse/catalog connectivity test
```

### Exit Criteria

- Nessie is healthy.
- Spark can reach the catalog endpoint.
- Warehouse path is correctly configured.
- Catalog state persists as intended.

---

# 14. Iceberg Bootstrap

## Phase 9 - Validate Spark -> Nessie -> Iceberg -> S3

### Objective

Prove the analytical storage foundation before business batch code is written.

### Minimum Test

```text
Spark
  â†“
Nessie
  â†“
Create Iceberg namespace
  â†“
Create Iceberg test table
  â†“
Write records
  â†“
Parquet files stored in S3-compatible storage
  â†“
Read the same Iceberg table
```

### Validate

- Table registration in Nessie.
- Metadata files in object storage.
- Parquet data files in object storage.
- Read-after-write.
- Schema evolution test.
- Snapshot behavior.

### Exit Criteria

The complete Spark/Nessie/Iceberg/S3 integration works reliably.

---

# 15. ClickHouse Foundation

## Phase 10 - Configure ClickHouse

### Objective

Provide low-latency operational and analytical serving.

### Infrastructure Sequence

```text
ClickHouse service
    â†“
Persistent volume
    â†“
HTTP endpoint
    â†“
Native endpoint
    â†“
Health check
```

### Initial Data Concepts

```text
delivery_events
vehicle_location_events

delivery_current_state
vehicle_current_state
shipment_current_state
route_current_state
```

These are conceptual names until the physical schemas are approved.

### Important Modeling Rule

```text
Immutable event history
!=
Latest operational state
```

### Exit Criteria

- ClickHouse is healthy.
- Storage is persistent.
- Approved database exists.
- Initial approved table design is documented.

---

# 16. Spark Runtime

## Phase 11 - Configure Spark

### Objective

Provide the distributed batch compute engine.

### Local Topology

```text
Spark Master
     |
     +---- Spark Worker
```

### Required Connectivity

```text
Spark -> Object Storage
Spark -> Nessie
Spark -> Iceberg
Spark -> ClickHouse where publication is approved
```

### Exit Criteria

- Spark Master is healthy.
- Worker is registered.
- Spark can read/write Iceberg through Nessie.
- Resource settings are appropriate for the local machine.

---

# 17. Airflow Foundation

## Phase 12 - Configure Airflow with LocalExecutor

### Objective

Provide workflow orchestration for daily batch processing.

### Approved Executor

```text
LocalExecutor
```

Do not introduce CeleryExecutor, Redis, RabbitMQ, or KubernetesExecutor unless explicitly approved later.

### Topology

```text
                PostgreSQL
                    ^
                    |
             Airflow Metadata
                    |
          +---------+---------+
          |                   |
     Webserver            Scheduler
                              |
                        LocalExecutor
                              |
                              v
                           Spark
```

### Recommended Sequence

```text
Airflow image/configuration
        â†“
PostgreSQL metadata connection
        â†“
Metadata DB migration/init
        â†“
Webserver
        â†“
Scheduler
        â†“
LocalExecutor validation
        â†“
Simple test DAG
```

### Exit Criteria

- Airflow UI is reachable.
- Scheduler is healthy.
- LocalExecutor runs a test task.
- Metadata persists in PostgreSQL.
- No unnecessary distributed executor dependencies exist.

---

# 18. Airflow -> Spark Integration

## Phase 13 - Validate Orchestration

### Objective

Prove that Airflow can orchestrate Spark without business complexity.

### Test Flow

```text
Airflow DAG
    â†“
Submit simple Spark job
    â†“
Spark executes
    â†“
Task success returned to Airflow
```

### Determine

- Spark submission mechanism.
- Retry behavior.
- Timeouts.
- Logs.
- Exit-code handling.
- Idempotency.

### Exit Criteria

A simple Spark job is successfully triggered, monitored, and reported by Airflow.

---

# 19. Flink Foundation

## Phase 14 - Configure Flink

### Objective

Provide the real-time stream-processing engine.

### Topology

```text
Kafka
  â†“
Flink JobManager
  â†“
Flink TaskManager
  â†“
ClickHouse
```

### Required Design Before Business Logic

- Event time.
- Processing time.
- Watermarks.
- Late events.
- Out-of-order events.
- Keying strategy.
- State backend.
- Checkpointing.
- Savepoints.
- Recovery.
- Delivery semantics.
- Windowing.
- Backpressure.
- Kafka source semantics.
- ClickHouse sink semantics.

### Exit Criteria

- JobManager is healthy.
- TaskManager is registered.
- Kafka connectivity works.
- ClickHouse connectivity works.

---

# 20. JSON Event Contracts

## Phase 15 - Define Versioned Event Contracts

### Objective

Create stable JSON contracts before producers and consumers are implemented.

### Canonical Envelope

```json
{
  "schema_version": 1,
  "event_id": "uuid",
  "event_type": "VEHICLE_DEPARTED",
  "event_timestamp": "2026-08-08T10:00:00Z",
  "order_id": "optional",
  "shipment_id": "optional",
  "delivery_id": "optional",
  "vehicle_id": "optional",
  "source": "event-producer",
  "payload": {}
}
```

### Initial Event Types

```text
ORDER_LOADED
VEHICLE_DEPARTED
IN_TRANSIT
DELIVERY_DELAYED
DELIVERED
```

### JSON Evolution Rules

- Prefer backward-compatible additive changes.
- New optional fields may be added when old consumers can ignore them.
- Never silently rename/remove fields.
- Never silently change field type or meaning.
- Breaking changes require a new explicit schema version.
- Required and optional fields must be documented.
- Consumers must deliberately handle unknown fields.
- Older supported schema versions must remain replayable.
- Compatibility tests must cover supported producer/consumer versions.

### Exit Criteria

- Event envelope is approved.
- Initial domain schemas are approved.
- Versioning rules are documented.
- Contract tests exist or are planned.

---

# 21. Synthetic Event Producer

## Phase 16 - Implement Logistics Event Generator

### Objective

Generate realistic local logistics activity.

### Generator Domains

```text
Orders
Shipments
Deliveries
Vehicles
Drivers
Warehouses
Routes
Transportation plans
GPS positions
Capacity
Delivery status transitions
```

### Flow

```text
Python Producer
      â†“
Versioned JSON Events
      â†“
Kafka
```

### Engineering Requirements

- Structured logging.
- Explicit config.
- Reproducible test data where useful.
- No hardcoded credentials.
- Type hints where useful.
- Clear package structure.
- Testable business logic.

### Exit Criteria

- Valid events are generated.
- Events can be published to Kafka.
- Event IDs are unique.
- Schema validation passes.

---

# 22. Streaming Pipeline

## Phase 17 - Implement Kafka -> Flink -> ClickHouse

### Objective

Build the first complete real-time processing path.

### Flow

```text
Producer
   â†“
Kafka
   â†“
Flink
   â†“
Deserialize JSON
   â†“
Validate
   â†“
Keyed/stateful processing
   â†“
ETA / delay / route logic
   â†“
ClickHouse
```

### Initial Streaming Capabilities

- Track vehicle location.
- Track delivery state.
- Track shipment state.
- Detect delayed deliveries.
- Calculate simplified ETA.
- Detect route deviation using approved local model.
- Monitor active workload.
- Identify problematic shipments.

### Validation

```text
Generate event
â†’ Kafka receives event
â†’ Flink processes event
â†’ ClickHouse contains expected result
```

### Exit Criteria

The real-time path works end to end.

---

# 23. Raw Historical Persistence

## Phase 18 - Persist Raw Data into Iceberg

### Objective

Create the replayable historical source.

### Candidate Raw Tables

```text
raw_delivery_events
gps_events
transportation_plans
warehouse_events
vehicle_events
shipment_events
delivery_confirmation_events
```

### Storage

```text
Iceberg table
    â†“
Parquet files
    â†“
S3-compatible object storage

Catalog:
Nessie
```

### Requirements

- Replay-safe writes.
- Idempotency.
- Event IDs.
- Schema evolution.
- Partition strategy.
- Snapshot retention.
- Compaction plan.
- Small-file strategy.

### Exit Criteria

- Raw event history is queryable through Iceberg.
- Data is replayable.
- Duplicate/retry behavior is understood.

---

# 24. Spark Batch Processing

## Phase 19 - Implement Daily Batch Computation

### Objective

Process completed and historical transportation data.

### Flow

```text
Iceberg Raw
    â†“
Spark
    â†“
Clean / Validate / Deduplicate
    â†“
Silver
    â†“
Aggregate
    â†“
Gold KPI Tables
```

### Minimum KPI Scope

- Completed deliveries.
- Total distance.
- Vehicle utilization.
- Planned vs actual delivery performance.
- Average delivery duration.
- Delayed deliveries.
- Delay rate.
- On-time delivery rate.
- Frequently delayed routes.
- Warehouse performance.
- Regional performance.
- Transportation cost.
- Cost per delivery.
- Historical delivery KPIs.
- Inputs for next-day planning.

### Candidate Outputs

```text
daily_delivery_kpi
vehicle_utilization_daily
route_performance_daily
warehouse_performance_daily
transportation_cost_daily
delivery_delay_analysis
```

Names remain conceptual until physical schemas are approved.

### Exit Criteria

- Spark reads Iceberg reliably.
- Batch outputs are deterministic.
- Reruns do not create incorrect duplicate results.
- KPI results are validated.

---

# 25. Airflow Daily Workflow

## Phase 20 - Implement Production-Style Daily DAG

### Objective

Move the proven Spark workflow under Airflow orchestration.

### Conceptual DAG

```text
check_source_readiness
        â†“
run_spark_daily_batch
        â†“
validate_outputs
        â†“
run_data_quality_checks
        â†“
publish_analytical_outputs
        â†“
publish_to_clickhouse_if_approved
        â†“
complete
```

### Determine

- Schedule.
- Timezone.
- Catchup.
- Backfill.
- Retries.
- Retry delays.
- Timeouts.
- Spark submission.
- Logging.
- Failure behavior.
- Idempotency.

### Exit Criteria

- Daily DAG succeeds.
- Failed tasks are visible.
- Safe retries work.
- Backfill works for approved historical dates.
- Duplicate analytical results are not created.

---

# 26. Analytical Publication

## Phase 21 - Publish Serving Data

### Objective

Expose analytical results efficiently for BI.

### Candidate Flow

```text
Spark Gold Iceberg Tables
          â†“
Approved Publication Step
          â†“
ClickHouse
          â†“
Superset
```

The exact publication model must be approved.

### Determine

- Which datasets stay only in Iceberg.
- Which datasets are published to ClickHouse.
- Refresh cadence.
- Incremental publication.
- Deduplication.
- Data freshness SLA.

### Exit Criteria

- BI-ready datasets exist.
- Publication is idempotent.
- Query latency is appropriate.

---

# 27. Superset

## Phase 22 - Prepare Superset Reporting

### Objective

Provide operational and management analytics.

Superset is a Docker Compose service in this local platform, and the repository includes a dedicated `superset-importer` service for BI-as-code imports.

The repository should provide:

- Approved analytical views/tables.
- Connection documentation.
- Version-controlled Superset BI assets.
- `make import-superset-assets` for importing database, dataset, chart, and dashboard definitions.
- Chart-level adhoc metric generation from readable YAML metric names.
- Dataset temporal metadata for BI time columns such as `business_date` and `event_hour`.
- Timeseries chart parameters such as `x_axis`, `granularity_sqla`, and `time_grain_sqla`.
- Refresh approach.
- Semantic-model guidance.
- KPI definitions.

## Operational Dashboard

Potential metrics:

- Active deliveries.
- Current vehicle locations.
- Current ETA.
- Delayed deliveries.
- Route deviations.
- Problematic shipments.
- Active vehicle workload.

## Management Dashboard

Potential metrics:

- Completed deliveries.
- On-time rate.
- Delayed rate.
- Average delivery duration.
- Vehicle utilization.
- Route performance.
- Warehouse performance.
- Regional performance.
- Transportation cost.
- Cost per delivery.
- Daily/weekly/monthly trends.

### Exit Criteria

- Superset can access approved serving data.
- Operational and management concerns are separated.
- KPI definitions match the approved business logic.
- Imported charts do not fail with missing saved metric errors.
- Timeseries charts have an explicit datetime column configuration.

---

# 28. Data Quality

## Phase 23 - Implement Critical Data Quality Rules

### Initial Checks

- Missing IDs.
- Duplicate event IDs.
- Missing timestamps.
- Invalid GPS coordinates.
- Unknown statuses.
- Invalid status transitions.
- Negative cost.
- Impossible capacity.
- Delivery event before shipment creation.
- Actual delivery before departure.
- Duplicate delivery confirmation.
- Late-arriving data.

Do not introduce a large data-quality framework unless justified.

### Exit Criteria

Critical data-quality failures are detectable and visible.

---

# 29. Idempotency, Replay, and Recovery

## Phase 24 - Validate Failure Safety

### Required Scenarios

```text
Same Kafka event twice
â†’ no incorrect duplicate business result

Flink restart
â†’ state recovers

Same Spark source twice
â†’ deterministic output

Airflow retry
â†’ no duplicate publication

Airflow backfill
â†’ correct historical partition/date

Kafka replay
â†’ derived state can be rebuilt

Iceberg replay/reprocessing
â†’ analytical tables can be recalculated
```

### Exit Criteria

The platform behaves safely under retry, restart, duplicate input, and historical replay.

---

# 30. Testing

## Phase 25 - Add Tests Incrementally

### Candidate Layers

```text
Unit tests
Integration tests
JSON contract tests
Kafka producer/consumer tests
Flink transformation tests
Spark transformation tests
Airflow DAG import tests
Airflow-to-Spark orchestration tests
PostgreSQL integration tests
Iceberg integration tests
Nessie catalog tests
ClickHouse integration tests
Data quality tests
Docker health tests
End-to-end tests
```

Do not implement every layer automatically.

### Exit Criteria

Tests exist where they protect critical behavior and architecture boundaries.

---

# 31. Observability

## Phase 26 - Add Practical Observability

### Minimum Signals

Kafka:

- Broker health.
- Consumer lag.
- Event throughput.
- Failed events.

Flink:

- Processing latency.
- Checkpoint status.
- Backpressure.
- Job failures.

Spark:

- Job duration.
- Failures.

Airflow:

- Scheduler health.
- DAG status.
- Task failures.
- Retries.
- Scheduling delay.

PostgreSQL:

- Availability.
- Connection usage where relevant.

ClickHouse:

- Availability.
- Ingestion latency.
- Query latency.

Data Lake:

- Write failures.
- Data freshness.

Business:

- Invalid event count.
- Delayed delivery count.

Do not automatically introduce Prometheus/Grafana.

### Exit Criteria

Major failures can be detected and debugged using the existing platform tooling.

---

# 32. Makefile and Developer Experience

## Phase 27 - Complete Developer Interface

### Desired Commands

```text
make help

make up
make down
make restart

make status
make ps
make health

make logs
make logs-kafka
make logs-flink
make logs-spark
make logs-airflow
make logs-clickhouse
make logs-postgres
make logs-storage
make logs-nessie

make console

make import-superset-assets
```

### Rules

- `make down` must not delete persistent data.
- `make clean` must explicitly state if it deletes persistent data.
- Destructive targets must never run automatically.

### Exit Criteria

A developer can operate the platform without remembering long Docker commands.
Superset BI assets can be imported from the repository without manual UI rebuilding.
The importer rebuilds the `superset-importer` image before running so code and YAML updates are used consistently.

---

# 33. Environment Acceptance Test

## Phase 28 - Validate the Complete Local Environment

The local environment is ready only when all approved services and integrations are healthy.

### Service Health

```text
Kafka          healthy
Flink          healthy
Spark          healthy
Airflow        healthy
PostgreSQL     healthy
ClickHouse     healthy
Object Storage healthy
Nessie         healthy
```

### Connectivity

```text
Kafka internal DNS works
Flink -> Kafka works
Flink -> ClickHouse works

Spark -> Object Storage works
Spark -> Nessie works
Spark -> Iceberg works
Iceberg -> Parquet files in object storage works

Airflow -> PostgreSQL works
Airflow -> Spark works

Superset approved analytical access is documented/tested
```

### Persistence

- PostgreSQL survives normal restart.
- ClickHouse survives normal restart.
- Kafka state behaves according to approved persistence.
- Object-storage data survives normal restart.
- Nessie metadata survives normal restart.

### Exit Criteria

Infrastructure is stable enough for end-to-end business validation.

---

# 34. End-to-End Business Demo

## Phase 29 - Validate the Full Business Scenario

### Streaming Scenario

```text
Baku Warehouse
      â†“
Truck with customer orders
      â†“
GPS + Delivery Events
      â†“
Versioned JSON
      â†“
Kafka
      â†“
Flink
      â†“
Delay / ETA / State Processing
      â†“
ClickHouse
      â†“
Operational Analytics
```

### Historical Scenario

```text
Raw Events
    â†“
Iceberg + Parquet
    â†“
Nessie
    â†“
Airflow LocalExecutor
    â†“
Spark
    â†“
Daily KPI Tables
    â†“
ClickHouse / Approved Serving
    â†“
Superset
```

### Expected Business Demonstration

The demo should prove:

- A vehicle can be tracked.
- Delivery state can be updated.
- A delayed delivery can be detected.
- Raw events are persisted.
- Historical events are replayable.
- Daily batch KPIs are calculated.
- Superset can consume approved analytical results.
- Restart/retry does not corrupt business results.

---

# 35. Final Definition of Done

The project is considered functionally complete when:

```text
[ ] Docker Compose starts the approved local platform.

[ ] Kafka accepts versioned JSON events.

[ ] Synthetic logistics events can be generated.

[ ] Flink processes real-time events.

[ ] Event-time, late-event, and out-of-order handling are implemented.

[ ] ClickHouse exposes current delivery and vehicle state.

[ ] Delay detection works.

[ ] ETA calculation works using the approved initial model.

[ ] Raw historical data is persisted in Apache Iceberg.

[ ] Iceberg stores Parquet data files in S3-compatible object storage.

[ ] Nessie manages the Iceberg catalog.

[ ] PostgreSQL reliably stores Airflow metadata.

[ ] Airflow uses LocalExecutor.

[ ] Airflow orchestrates Spark batch processing.

[ ] Spark calculates approved daily transportation KPIs.

[ ] Analytical publication is idempotent.

[ ] Superset can consume approved analytical datasets.

[ ] Duplicate inputs do not create incorrect business results.

[ ] Kafka/Data Lake replay is possible.

[ ] Flink recovery works.

[ ] Spark reruns are deterministic.

[ ] Airflow retries/backfills are safe.

[ ] Persistent data survives normal restart.

[ ] `make health` reports major service readiness.

[ ] Individual service logs are easy to access.

[ ] End-to-end streaming and batch scenarios pass.
```

---

# 36. Recommended Exact Implementation Order

```text
00  Read requirement.md
01  Read plan.md
02  Inspect repository
03  Resolve remaining ADRs

04  Repository foundation
05  .gitignore
06  .env.example
07  Docker Compose foundation
08  Shared network / volumes
09  Makefile foundation

10  PostgreSQL
11  Kafka
12  Object Storage
13  Nessie
14  Spark
15  Spark -> Nessie -> Iceberg -> S3 validation
16  ClickHouse
17  Airflow LocalExecutor
18  Airflow -> Spark validation
19  Flink

20  JSON event contracts
21  Synthetic event producer
22  Producer -> Kafka validation
23  Kafka -> Flink validation
24  Flink -> ClickHouse validation

25  Raw Iceberg persistence
26  Spark Silver transformations
27  Spark Gold KPI transformations
28  Airflow daily DAG
29  Analytical publication

30  Superset serving preparation

31  Data quality
32  Idempotency
33  Replay
34  Recovery
35  Unit/integration tests
36  End-to-end tests

37  Observability improvements
38  Final Makefile developer experience
39  Documentation cleanup
40  Final end-to-end business demo
```

Never execute multiple items automatically because they appear adjacent in this list.

---

# 37. Codex Kickoff Prompt

Use the following prompt in Codex after `requirement.md` and `plan.md` are present in the repository:

```text
Read requirement.md and plan.md completely.

requirement.md is the mandatory source of truth for business,
architecture, infrastructure, engineering, security, and approval rules.

plan.md defines the implementation sequence.

The following architecture choices are FINAL and must not be reopened
unless I explicitly request a change:

- Streaming serialization/event contracts: JSON
- Schema evolution: versioned JSON contracts
- Data Lake data-file format: Parquet
- Data Lake table format: Apache Iceberg
- Iceberg catalog: Nessie
- Airflow executor: LocalExecutor
- BI platform: Superset
- PostgreSQL: Airflow metadata and approved relational/reference workloads
- ClickHouse: low-latency operational and analytical serving
- Spark: distributed batch compute
- Flink: real-time stream processing
- Kafka: streaming event backbone
- Airflow: orchestration/control plane only

Follow the strict one-action-at-a-time approval workflow.

Before every meaningful action:

1. Explain the current state.
2. Recommend exactly one next step.
3. Explain why.
4. List files/services affected.
5. State risk: Low / Medium / High.
6. Show the exact command when a command is required.
7. State impact:
   Read-only / Non-destructive / Changes files / Starts services / Destructive.
8. Explain the expected result.
9. Ask for my explicit approval.
10. Stop.

Approval applies only to the immediately preceding action.

Never automatically execute the next phase.
Never silently modify unrelated files.
Never automatically retry failed operations.
Never perform destructive operations without immediate explicit confirmation.

Start ONLY by proposing a read-only repository inspection.
Show the exact commands you want to use.
Do not execute them until I approve.
```

---

# 38. Final Rule

Think proactively about:

- Architecture.
- Scalability.
- Reliability.
- Performance.
- Replayability.
- Idempotency.
- Schema evolution.
- Security.
- Observability.
- Local resource usage.
- Developer experience.
- Production readiness.

But never perform the next meaningful implementation action without explicit approval.
