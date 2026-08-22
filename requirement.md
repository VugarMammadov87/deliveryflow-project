# Real-Time Delivery Monitoring and Daily Transportation Optimization

> **Document purpose:** This file is the single source of truth for the project's business requirements, architecture requirements, infrastructure requirements, development rules, and implementation constraints. The platform is intended for local development first, while applying production-grade data engineering principles where practical.

## 1. Project Overview

The company manages transportation and deliveries from multiple warehouses to stores and customers.

The platform has two primary objectives:

1. Monitor active delivery operations in near real time.
2. Analyze historical transportation data using daily batch processing to improve future transportation planning.

The architecture combines:

```
Real-time operational processing
+
Historical analytical processing
+
Transportation optimization
+
BI reporting

```

The platform should initially be designed for local development using Docker Compose, while following production-grade data engineering principles where practical.

---

## 2. Business Objectives

The main business goals are:

- Track active deliveries.
- Track current vehicle locations.
- Track shipment and order status.
- Detect delivery delays quickly.
- Calculate estimated arrival time.
- Detect route deviation.
- Monitor vehicle capacity.
- Monitor active vehicle workload.
- Detect problematic shipments.
- Generate operational alerts.
- Measure delivery performance.
- Compare planned vs actual delivery performance.
- Analyze warehouse performance.
- Analyze regional performance.
- Analyze route performance.
- Calculate transportation cost.
- Improve vehicle utilization.
- Improve route planning.
- Improve next-day transportation planning.
- Provide operational and management BI dashboards.

---

## 3. Core Business Entities

The following conceptual entities define the initial business domain.

At minimum consider:

```
Order
Shipment
Delivery
Vehicle
Driver
Warehouse
Customer
Store
Route
Transportation Plan
Delivery Event
GPS Event
Region
Delivery Confirmation

```

Important identifiers may include:

```
order_id
shipment_id
delivery_id
vehicle_id
driver_id
warehouse_id
customer_id
store_id
route_id
transportation_plan_id
event_id

```

These are conceptual requirements.

The final physical schemas must be discussed before implementation.

---

## 4. End-to-End Business Process

The end-to-end business flow is defined below.

Reference end-to-end flow:

```
Orders / Warehouse / Vehicles
            |
            v
     Transportation Plan
            |
            v
        Deliveries
            |
     +------+------+
     |             |
     v             v
Streaming       Raw Data
Pipeline        Storage
     |             |
     v             v
Kafka           Data Lake
     |             |
     v             v
Flink           Spark
     |             |
     v             v
Live Delivery   Daily Delivery
Monitoring      Analytics
     |             |
     +------+------+
            |
            v
        BI Reporting

```

Explain the flow clearly.

Before transportation begins:

```
Orders
+
Warehouse information
+
Vehicle availability
+
Capacity
+
Routes
        |
        v
Transportation Plan

```

During transportation:

```
Vehicles / Deliveries
        |
        v
Events
        |
        v
Kafka
        |
        v
Flink
        |
        v
Operational Serving Layer

```

At the same time raw/historical information must be persisted for replay and analytics:

```
Events / Operational Data
        |
        v
Data Lake
        |
        v
Spark Daily Batch
        |
        v
Historical / Analytical Layer

```

Both operational and historical outputs eventually support BI and transportation decision-making. Apache Airflow should coordinate approved scheduled batch workflows around Spark, data-quality checks, and downstream publication, while remaining outside the streaming data path.

---

## 5. Delivery Lifecycle

The platform must support a delivery lifecycle.

Initial event examples:

```
ORDER_LOADED
VEHICLE_DEPARTED
IN_TRANSIT
DELIVERY_DELAYED
DELIVERED

```

Future events may include concepts such as:

```
ORDER_ASSIGNED
ARRIVED_AT_CUSTOMER
DELIVERY_FAILED
DELIVERY_CANCELLED
RETURN_STARTED
RETURN_COMPLETED
VEHICLE_BREAKDOWN
ROUTE_DEVIATION

```

Do not treat the future list as finalized.

The final event model must be discussed before implementation.

---

## 6. Real-Time Streaming Requirements

During active delivery operations, events are continuously generated.

Potential sources:

- Vehicle GPS devices
- Transportation management systems
- Warehouse systems
- Mobile driver applications
- Delivery services
- Order management systems

Examples of streaming data:

```
GPS locations
Shipment status updates
Warehouse departure events
Delivery confirmation events
Vehicle capacity changes
Transportation plan updates

```

The streaming architecture should conceptually follow:

```
Event Producer
    |
    v
Kafka
    |
    v
Flink
    |
    v
Operational Serving Layer

```

---

## 7. Apache Kafka Requirements

Apache Kafka should act as the streaming event backbone.

Potential topics may eventually include:

```
delivery-events
vehicle-location-events
shipment-status-events
warehouse-events
transportation-plan-events

```

These are examples only.

Do not consider the topic architecture finalized.

Before implementation, the following must be designed:

- Topic strategy
- Event domain boundaries
- Message key
- Partition key
- Partition count
- Replication factor
- Event ordering requirements
- Retention
- Consumer groups
- Serialization format: JSON
- Versioned JSON event contracts
- Event schema
- JSON schema evolution and compatibility rules
- Replay strategy
- Duplicate handling
- Delivery semantics
- Dead-letter handling where justified

Kafka must support replay of historical events where required.

---

## 8. Apache Flink Requirements

Apache Flink should process transportation events in real time.

Primary responsibilities:

- Track current location of every active vehicle.
- Track shipment state.
- Track delivery status.
- Detect late deliveries.
- Calculate ETA.
- Detect route deviation.
- Monitor vehicle utilization.
- Monitor active delivery workload.
- Identify problematic shipments.
- Generate alerts.

Before implementing Flink jobs, explicitly design:

- Event time
- Processing time
- Watermarks
- Late events
- Out-of-order events
- Stateful processing
- State backend
- Keying strategy
- Checkpointing
- Savepoints
- Recovery behavior
- Exactly-once requirements
- At-least-once implications
- Windowing
- Backpressure handling
- Kafka source semantics
- Sink semantics

Do not arbitrarily choose Flink SQL, Table API, Java DataStream API, or PyFlink without an architectural decision.

---

## 9. Operational Serving Layer

Real-time processed data should be available through a low-latency serving layer.

ClickHouse is the initial candidate for operational and analytical serving.

Potential data concepts:

```
delivery_current_state
vehicle_current_state
shipment_current_state
route_current_state
delivery_events

```

These are examples only.

Before implementation determine:

- Query and access patterns
- Table boundaries and schemas
- ClickHouse table engines
- `ORDER BY` keys
- Partitioning strategy
- Primary key strategy
- TTL and retention requirements
- Cardinality and expected data volume
- Insert batching strategy
- Update, replacement, and deduplication semantics
- Version columns where required
- Materialized views where justified
- Secondary indexes or projections only where justified
- Consistency expectations
- Flink-to-ClickHouse delivery semantics
- Retry and replay behavior
- Operational vs historical query responsibilities

ClickHouse should primarily serve low-latency operational and analytical queries. The Data Lake remains the replayable raw and historical storage layer unless a separate architecture decision explicitly changes that responsibility.

---

## 10. Real-Time Delay Detection

The system should be capable of detecting delivery delays.

Potential signals:

- Planned departure time vs actual departure time
- Planned ETA vs calculated ETA
- Expected route progress vs GPS position
- Traffic-related delay signals if eventually available
- Delivery stop duration
- Historical route duration
- Driver/vehicle status
- Delivery sequence

Example:

```
Planned arrival: 14:00
Current ETA:     14:25
Delay:           25 minutes

```

The exact business definition of "delayed" must be defined before implementation.

Do not hardcode an arbitrary threshold without approval.

---

## 11. Route Deviation Detection

The platform should eventually support route deviation detection using GPS information.

Potential inputs:

```
planned route
current GPS location
previous GPS locations
destination
planned stop sequence

```

Implementation method is not finalized.

Possible approaches may eventually include:

- Geofencing
- Distance from expected route
- Road network integration
- Map API
- Simplified route-coordinate model for the local lab

A suitable approach must be discussed before implementation.

---

## 12. ETA Calculation

The streaming platform should eventually calculate or estimate arrival time.

Potential inputs:

```
Current GPS location
Destination
Remaining distance
Historical route duration
Current delay
Vehicle speed
Planned route

```

Initial lab implementation can use a simplified ETA model.

The architecture should allow more advanced models later.

---

## 13. Raw Data Storage

All relevant raw transportation events must eventually be persisted so they can be:

- Reprocessed
- Replayed
- Audited
- Analyzed historically
- Used for debugging
- Used for data quality investigation

Potential raw datasets:

```
raw_delivery_events
gps_events
transportation_plans
warehouse_events
vehicle_events
shipment_events
delivery_confirmation_events

```

---

## 14. Data Lake Requirements

Historical and raw information should be stored in an S3-compatible Data Lake.

Possible technologies:

```
RustFS
MinIO
S3-compatible object storage
AWS S3 in future production scenarios

```

The exact implementation must be approved before creation.

The Data Lake architecture should evaluate:

```
Raw / Bronze
Silver
Gold

```

where appropriate.

The approved analytical storage model is:

```
Data files    -> Parquet
Table format  -> Apache Iceberg
Catalog       -> Nessie
Object store  -> Approved S3-compatible storage

```

Before implementation determine:

- Parquet data-file configuration
- Apache Iceberg table design
- Nessie catalog configuration
- Partitioning
- Data retention
- Schema evolution
- Small-file strategy
- Compaction
- Replay
- Idempotency
- Data lifecycle
- Data quality
- Metadata management

---

## 15. Apache Iceberg

Apache Iceberg is the approved table format for analytical Data Lake tables. Parquet remains the underlying analytical data-file format.

Required architectural benefits include:

- ACID-style table commits
- Snapshot management
- Schema evolution
- Partition evolution
- Hidden partitioning
- Metadata-driven query planning
- Time travel
- Maintainable large analytical datasets
- Safer replay, backfill, and batch publication patterns

The implementation must design Iceberg tables intentionally rather than treating Iceberg as a transparent replacement for plain files.

Before creating production-style Iceberg tables determine:

- Table boundaries and ownership
- Partition strategy
- Sort/order strategy where useful
- Snapshot retention
- Expired snapshot cleanup
- Orphan file cleanup
- Compaction strategy
- Schema evolution rules
- Partition evolution rules
- Spark read/write behavior
- Flink integration only where justified
- Nessie catalog namespaces and references

Plain standalone Parquet tables are not the target analytical table architecture for this project. Parquet is used as the Iceberg data-file format.

---

## 16. Catalog Layer

Nessie is the approved catalog layer for Apache Iceberg tables.

The architecture must clearly distinguish:

```
Data Files     -> S3-compatible Object Storage (Parquet)
Iceberg Metadata -> S3-compatible Object Storage
Catalog        -> Nessie
Compute        -> Spark / Flink where supported and approved
Orchestration  -> Airflow

```

Nessie is responsible for catalog-level table discovery and metadata references; it is not the Data Lake itself and must not store business data as a replacement for object storage.

Before implementation determine:

- Nessie image/version compatibility with Iceberg and Spark
- Catalog endpoint configuration
- Warehouse location
- Namespace strategy
- Authentication strategy for future non-local environments
- Branch/reference usage, keeping local development simple
- Recovery and metadata persistence behavior
- Health-check/readiness behavior

Do not introduce a second metadata catalog unless a separate approved requirement justifies it.

---

## 17. Daily Batch Processing and Workflow Orchestration

At the end of each business day, Apache Airflow should orchestrate the approved batch workflow, while Apache Spark remains the primary batch compute engine for processing raw and completed transportation information.

Airflow is the orchestration/control plane and must not replace Flink for streaming processing or Spark for distributed batch computation.

Conceptual daily workflow:

```
Airflow DAG
    |
    +--> Validate upstream data readiness
    |
    +--> Trigger Spark daily batch
    |
    +--> Run approved data-quality checks
    |
    +--> Publish analytical outputs
    |
    +--> Trigger downstream BI/optimization dependencies where justified

```

Airflow responsibilities may include:

- Scheduling daily and periodic batch workflows.
- Managing task dependencies.
- Triggering Spark jobs.
- Coordinating data-quality and publication steps.
- Managing retries and failure visibility.
- Supporting approved backfills and reprocessing.
- Exposing DAG and task execution status.
- Applying timeouts, retry policies, and execution controls.

Before implementing Airflow, explicitly determine:

- Executor: LocalExecutor.
- LocalExecutor parallelism and concurrency limits appropriate for the local machine.
- Airflow metadata database design.
- PostgreSQL database/schema/user isolation for Airflow metadata.
- DAG structure and ownership.
- Scheduling and timezone behavior.
- Catchup and backfill behavior.
- Retry and timeout strategy.
- Idempotency requirements.
- Spark job submission mechanism.
- Connection and secret management.
- Logging and observability.
- Failure notification strategy where justified.

Input domains for the Spark batch layer may include:

```
Raw delivery events
GPS events
Transportation plans
Warehouse data
Vehicle data
Shipment records
Completed deliveries
Route information

```

---

## 18. Apache Spark Batch Requirements

Spark should remain responsible for distributed batch computation. Approved production-style daily Spark jobs should normally be triggered and monitored by Airflow rather than relying on manual scheduling.

Spark should calculate at minimum:

- Number of completed deliveries
- Total distance traveled
- Vehicle utilization
- Planned vs actual delivery time
- Average delivery duration
- Delayed deliveries
- Delay rate
- On-time delivery rate
- Frequently delayed routes
- Warehouse performance
- Regional performance
- Transportation cost
- Cost per delivery
- Historical delivery KPIs
- Inputs for next-day planning

Potential analytical outputs:

```
daily_delivery_kpi
vehicle_utilization_daily
route_performance_daily
warehouse_performance_daily
transportation_cost_daily
delivery_delay_analysis

```

These names are conceptual and must not be treated as final schemas.

---

## 19. Vehicle Utilization

The system should calculate vehicle utilization.

Example conceptual formula:

```
Utilization =
Used Capacity / Available Capacity

```

Capacity may eventually be measured using:

```
Order count
Weight
Volume
Pallet count
Custom capacity unit

```

The final business definition must be decided before implementation.

---

## 20. Planned vs Actual Performance

The analytical layer should support comparisons such as:

```
planned_departure_time vs actual_departure_time
planned_arrival_time   vs actual_arrival_time
planned_duration       vs actual_duration
planned_distance       vs actual_distance
planned_capacity       vs actual_capacity

```

These comparisons should enable root-cause and route-performance analysis.

---

## 21. Transportation Cost Analytics

The analytical platform should eventually support transportation cost calculations.

Potential cost components:

```
Fuel
Driver cost
Vehicle operating cost
Distance-related cost
Fixed trip cost
Warehouse handling cost
Other operational costs

```

Exact formulas must be defined with business requirements before implementation.

---

## 22. Next-Day Transportation Optimization

Daily batch output should eventually be usable as input for the next day's transportation planning.

Potential use cases:

- Better vehicle allocation
- Better route selection
- Avoid historically problematic routes
- Better warehouse allocation
- Capacity balancing
- Improved delivery scheduling

Initial project scope may provide analytical inputs rather than implementing a full optimization solver.

Do not introduce optimization frameworks without explicit approval.

---

## 23. Example Business Scenario

Reference business scenario:

A truck leaves the Baku warehouse carrying 80 customer orders.

During transportation:

```
Vehicle
  |
  v
GPS + Delivery Events
  |
  v
Kafka
  |
  v
Flink

```

Flink detects that the vehicle is approximately 25 minutes behind schedule.

Several active shipments are marked as potentially delayed.

At the end of the day Spark processes completed delivery data.

Example daily results:

```
Delivered orders:             1,250
On-time delivery rate:        94%
Delayed delivery rate:         6%
Average vehicle utilization:  82%
Average delivery time:        46 minutes
Highest-delay route:          Baku-Sumqayit

```

These results can then be used to improve the next day's transportation plan.

---

## 24. BI Reporting Requirements

Superset is the approved BI and reporting layer.

Potential KPIs:

- Total deliveries
- Completed deliveries
- Active deliveries
- On-time delivery rate
- Delayed delivery rate
- Average delivery time
- Vehicle utilization rate
- Total distance traveled
- Delivery performance by region
- Delivery performance by warehouse
- Most delayed routes
- Transportation cost
- Transportation cost per delivery
- Planned vs actual delivery time
- Daily trend
- Weekly trend
- Monthly trend

Operational dashboards and management dashboards should be considered separate concerns where appropriate.

Superset BI objects should be importable from version-controlled repository assets where practical.

Required local BI-as-code command:

```
make import-superset-assets

```

This command should import or update the Superset database connection, datasets, charts, and dashboard definitions from the repository assets instead of requiring manual dashboard rebuilding in the Superset UI.

Superset chart definitions must be renderable after import:

- YAML metric names such as `event_count`, `delay_rate`, and `avg_delay_minutes` must not be treated as missing saved metrics.
- The importer should convert readable metric names into chart-level adhoc metrics.
- Count and amount columns should use `SUM(...)` unless a different aggregation is explicitly required.
- Rate and average columns should use `AVG(...)` unless a different aggregation is explicitly required.
- Timeseries charts must define a datetime column explicitly.
- `event_hour` must be configured as the datetime axis for hourly delivery event volume.
- `business_date` and `event_hour` should be represented as temporal dataset metadata where appropriate.

---

## 25. Target Logical Architecture

The target logical architecture is:

```
                    +----------------------+
                    |   Orders / Warehouse |
                    |      / Vehicles      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Transportation Plan  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      Deliveries      |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
        +----------------+          +----------------+
        |     Kafka      |          |   Data Lake    |
        +-------+--------+          +-------+--------+
                |                           |
                v                           v
        +----------------+          +----------------+       +----------------+
        |     Flink      |          |     Spark      |<------+    Airflow     |
        +-------+--------+          +-------+--------+       | orchestration  |
                |                           |                +----------------+
                |                           |
                v                           v
        +-------------------+       +-------------------+
        | ClickHouse / Live |       |  Historical KPI   |
        |   Serving Layer   |       |    / Analytics    |
        +---------+---------+       +---------+---------+
                |                           |
                +-------------+-------------+
                              |
                              v
                     +----------------+
                     |    Superset    |
                     +----------------+

```

Airflow represents the batch workflow orchestration/control plane. Business data should flow between storage and compute systems directly; Airflow should coordinate Spark execution, dependencies, retries, data-quality steps, and downstream publication rather than becoming a data-processing engine itself.

---

## 26. Proposed Technology Stack

The approved target technology stack is:

```
Container Runtime:
Docker

Local Orchestration:
Docker Compose

Streaming Backbone:
Apache Kafka

Stream Processing:
Apache Flink

Batch Processing:
Apache Spark

Workflow Orchestration:
Apache Airflow

Low-Latency Serving:
ClickHouse

Data Lake:
S3-compatible object storage

Analytical Data File Format:
Parquet

Data Lake Table Format:
Apache Iceberg

Iceberg Catalog:
Nessie

Programming:
Python
PySpark
Flink-compatible implementation to be decided

Relational Database:
PostgreSQL where business/relational workloads require it, and as the initial candidate for Airflow metadata storage

BI:
Superset

Developer Automation:
Makefile

```

This list represents the approved target technologies. It does not authorize Codex to install or configure all services automatically; the strict approval workflow still applies.

---

## 27. Repository Infrastructure Requirements

The repository should eventually contain professional local infrastructure configuration.

Potential structure:

```
project-root/
â”œâ”€â”€ requirement.md
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
â”‚
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ producers/
â”‚   â”œâ”€â”€ streaming/
â”‚   â””â”€â”€ batch/
â”‚
â”œâ”€â”€ configs/
â”œâ”€â”€ scripts/
â”œâ”€â”€ tests/
â”œâ”€â”€ docs/
â””â”€â”€ data/

```

This is a possible target structure only.

The actual existing repository must be inspected before creating these directories.

---

## 28. Docker Compose Requirements

Docker Compose must eventually manage all approved local platform services.

Compose configuration should follow these principles:

- Modular service design
- Explicit service definitions
- Shared Docker network
- Named volumes
- Persistent storage where required
- Health checks
- Restart policies where justified
- Environment-driven configuration
- Service dependencies
- Internal Docker DNS
- Host port mappings only where required
- Version-pinned images
- Resource configuration where justified
- Minimal duplication
- Local developer usability

Do not unnecessarily expose internal services to the host.

---

## 29. Docker Internal Networking

Containers must communicate using Docker DNS/service names.

Examples:

```
kafka:9092
clickhouse:8123
postgres:5432
airflow-webserver:8080
nessie:19120
spark-master:7077

```

Do not configure one container to access another using:

```
localhost

```

because inside a container `localhost` refers to that container itself.

Clearly separate:

```
Internal container endpoint
vs
Host-accessible endpoint

```

---

## 30. Shared Docker Network

The data platform should use a shared Docker network.

Potential name:

```
delivery-platform-net

```

Before creating it, inspect whether an appropriate network already exists.

The architecture should account for:

- Service discovery
- Container DNS
- Host access
- Port conflicts
- Multiple local instances
- Network isolation

---

## 31. Docker Volumes

Persistent state should use named volumes where appropriate.

Examples:

```
Kafka data
ClickHouse data
Object Storage data
PostgreSQL data
Airflow metadata through PostgreSQL
Airflow logs where persistent local logs are justified
Nessie metadata

```

Normal:

```
make down

```

must not delete persistent volumes.

Destructive cleanup should be a separate explicit action.

---

## 32. Container API and Management Requirements

Docker Engine / Docker Compose should be the primary container management mechanism.

Required operational capabilities:

- Container status
- Container health
- Logs
- Start
- Stop
- Restart
- Service inspection
- Network inspection
- Volume inspection

Prefer standard commands such as:

```
docker compose
docker inspect
docker logs
docker network inspect
docker volume inspect

```

Do not mount:

```
/var/run/docker.sock

```

inside application containers unless a genuine Container API use case exists.

Docker socket access provides powerful host-level privileges and must be treated as a security-sensitive architectural decision.

If direct Container API access is proposed, document:

- Why it is required
- Security implications
- Alternatives
- Required permissions

and obtain explicit approval before implementing it.

---

## 33. Makefile Requirements

Create a professional root `Makefile` during the implementation phase.

The goal is to provide a simple developer interface.

Expected commands where appropriate:

```
make help

make up
make down
make restart

make status
make ps
make health

make logs

make build
make pull

make clean

make import-superset-assets

```

`make import-superset-assets` should rebuild the importer image before running the importer container so BI asset and script changes are not hidden by a stale Docker image.

Service-specific logging may include:

```
make logs-kafka
make logs-flink
make logs-spark
make logs-airflow
make logs-clickhouse
make logs-postgres
make logs-storage
make logs-nessie

```

Service-specific start/stop targets should only be introduced where they provide real value.

---

## 34. Destructive Makefile Operations

Commands such as:

```
make clean
make reset
make reset-kafka
make reset-lake

```

must clearly state when they delete:

- Containers
- Volumes
- Topics
- Object storage data
- Database data
- Metadata

Destructive commands must never be executed automatically.

---

## 35. Environment Variable Requirements

Environment-specific values must be centralized.

Primary candidate:

```
.env

```

Optional supporting files:

```
.env.example
.env.local
.env.dev

```

The final environment file strategy must be decided before implementation.

Potential environment variables include only those actually required.

Examples:

```
COMPOSE_PROJECT_NAME

KAFKA_IMAGE
KAFKA_BROKER_PORT
KAFKA_CONTROLLER_PORT
KAFKA_CLUSTER_ID

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

Do not create environment variables that are not used.

---

## 36. Environment Configuration Principles

Avoid hardcoded:

- Ports
- Hosts
- Container image versions
- Credentials
- Endpoints
- Bucket names
- Database names

where environment configuration is more appropriate.

Use Compose interpolation:

```
${VARIABLE_NAME}

```

or safe local defaults where justified:

```
${VARIABLE_NAME:-default-value}

```

Do not provide insecure production secret defaults.

---

## 37. Internal vs External Endpoints

Explicitly distinguish endpoints.

Example:

Host access:

```
localhost:9092
localhost:8123
localhost:5432
localhost:8080
localhost:19120

```

Container access:

```
kafka:9092
clickhouse:8123
postgres:5432
airflow-webserver:8080
nessie:19120

```

Kafka may require distinct internal and external listener configuration.

Potential variables:

```
KAFKA_INTERNAL_BOOTSTRAP_SERVERS
KAFKA_EXTERNAL_BOOTSTRAP_SERVERS

```

The final listener configuration must be designed based on the chosen Kafka image.

---

## 38. Secrets Management

Never commit real production credentials.

Local development credentials must be clearly identified as:

```
LOCAL / DEVELOPMENT ONLY

```

When appropriate provide `.env.example` with placeholders such as:

```
S3_ACCESS_KEY=change-me
S3_SECRET_KEY=change-me

```

Existing `.gitignore` must be inspected before modification.

Never overwrite `.gitignore` blindly.

---

## 39. Docker Image Versioning

Do not arbitrarily use:

```
latest

```

for important platform services.

Pin compatible versions.

Before selecting versions, validate compatibility between:

```
Kafka <-> Flink connector
Flink <-> Java
Spark <-> Java
Spark <-> Iceberg
Iceberg <-> Spark
Airflow <-> Python
Airflow <-> PostgreSQL
Airflow <-> Spark job submission
Nessie <-> Iceberg
Python <-> Kafka client
PostgreSQL <-> Python database driver

```

Version choices should be documented.

---

## 40. Health Checks

Where possible every major service should have a meaningful health check.

Health should represent service readiness, not merely process existence.

Examples:

```
Kafka broker ready
Flink JobManager reachable
Spark Master reachable
Airflow webserver/API reachable
Airflow scheduler healthy
PostgreSQL readiness query successful
ClickHouse readiness query successful
Object storage responding
Nessie API responding

```

---

## 41. Make Health

The final developer experience should support:

```
make health

```

with a simple result similar to:

```
Kafka           healthy
Flink           healthy
Spark           healthy
Airflow         healthy
PostgreSQL      healthy
ClickHouse      healthy
Object Storage  healthy
Nessie          healthy

```

Do not introduce a complex health-check framework just for local development.

---

## 42. Logging Requirements

The developer should be able to run:

```
make logs

```

for platform logs.

Service-specific commands may include:

```
make logs-kafka
make logs-flink
make logs-spark
make logs-airflow
make logs-clickhouse
make logs-postgres

```

Prefer:

```
docker compose logs -f

```

and other standard Docker tooling.

---

## 43. Service Dependencies

Model startup dependencies carefully.

Example conceptual dependencies:

```
Kafka
  |
  v
Flink

PostgreSQL
  |
  +----> Airflow metadata database

Airflow
  |
  +----> Spark job orchestration

Object Storage
  |
  +----> Spark data read/write
  |
  +----> Iceberg / Nessie metadata and data dependencies

```

Do not assume `depends_on` means the dependency is application-ready.

Where necessary use:

- Health checks
- Retry logic
- Readiness logic

---

## 44. Local Resource Management

This project must remain usable on a local development machine.

Avoid assigning excessive:

```
CPU
RAM
Disk
Kafka retention
Spark executors
Flink task slots
Airflow scheduler/webserver resources
PostgreSQL memory and connection limits

```

without understanding available host resources.

Where appropriate make important resource values configurable through `.env`.

---

## 45. Developer Experience

The target local workflow should eventually resemble:

```
git clone ...
cd project

cp .env.example .env

make up
make health
make status
make console

```

Debugging:

```
make logs
make logs-kafka
make logs-flink
make logs-spark
make logs-airflow
make logs-postgres

```

Stopping the environment:

```
make down

```

Full destructive reset:

```
make clean

```

`make clean` must clearly be identified as destructive if it removes persistent data.

---

## 46. Service Access Information

Consider a command such as:

```
make console

```

to show available service endpoints.

Potential example:

```
Kafka Bootstrap: localhost:9092
Flink UI:        http://localhost:<port>
Spark UI:        http://localhost:<port>
Airflow UI:      http://localhost:8080
PostgreSQL:      localhost:5432
ClickHouse HTTP: http://localhost:8123
Nessie API:      http://localhost:19120
Object Storage:  http://localhost:9000

```

Only services actually present in the environment should be shown.

---

## 47. Configuration Validation

After approved Docker Compose changes, recommend:

```
docker compose config

```

before starting services.

Validation commands are separate execution actions and require separate user approval.

Creating a file does not automatically authorize running validation.

---

## 48. Python Engineering Requirements

Python code must follow professional data engineering standards.

Requirements:

- Clear package structure
- Type hints where useful
- Focused functions
- Separation of infrastructure and business logic
- Explicit configuration
- Structured logging
- Intentional exception handling
- Idempotent processing where required
- No hardcoded credentials
- No hardcoded environment-specific paths
- Testable transformation logic
- Minimal unnecessary abstractions

Do not create Python services automatically.

---

## 49. PostgreSQL Requirements

PostgreSQL should be the relational database candidate where business/reference workloads require a relational store. It is also the initial candidate for Airflow metadata storage.

Airflow metadata and project business/reference data should not be mixed indiscriminately. Where the same local PostgreSQL instance is reused, use separate databases and/or roles with clear ownership and access boundaries.

Important PostgreSQL engineering requirements include:

- SQL keywords uppercase.
- Prefer lowercase `snake_case` identifiers and avoid unnecessary quoted mixed-case identifiers.
- Every `SELECT` column on its own line.
- Every `GROUP BY` column on its own line.
- Every `ORDER BY` column on its own line.
- Functions remain on a single line where readable.
- Nested functions remain on a single line where readable.
- Window functions remain on a single line.
- Use appropriate PostgreSQL data types, including `TIMESTAMPTZ` for event timestamps where timezone-aware semantics are required.
- Create indexes only for validated access patterns.
- Use transactions intentionally.
- Use `INSERT ... ON CONFLICT` for idempotent upsert patterns where appropriate.
- Apply connection pooling only where the workload justifies it.
- Use least-privilege database roles.
- Do not hardcode database credentials in source code.

Example window function:

```
ROW_NUMBER() OVER (PARTITION BY delivery_id, shipment_id ORDER BY event_timestamp DESC, event_id DESC) AS row_number

```

Do not format window functions across multiple lines.

Example PostgreSQL date expression:

```
DATE '1980-01-01' + offset_day AS delivery_date

```

SQL should be:

```
Production-grade
Compact
Readable
SARGable where applicable
Performance-conscious
Maintainable

```

Avoid unnecessary:

```
SELECT *
DISTINCT
CTEs
Subqueries
JOINs
ORDER BY
FULL JOIN
Correlated subqueries

```

These constructs are not forbidden; use them when they are the clearest and most performant solution for the business requirement. Existing business logic must not be changed without an explicit reason.

Before using PostgreSQL for a new workload, determine:

- Whether PostgreSQL is a source-of-truth database, metadata store, reference store, or serving dependency.
- Database/schema boundaries.
- Role and permission model.
- Primary keys and uniqueness requirements.
- Index strategy.
- Transaction and isolation requirements.
- Retention and cleanup requirements.
- Backup/recovery expectations for non-local environments.
- Airflow metadata database isolation.
- Connection limits and pooling requirements.

---

## 50. Data Quality Requirements

The platform should eventually consider validation for:

- Missing IDs
- Duplicate event IDs
- Missing timestamps
- Invalid GPS coordinates
- Unknown delivery statuses
- Invalid status transitions
- Negative cost
- Impossible capacity values
- Delivery events arriving before shipment creation
- Actual delivery timestamp before departure
- Duplicate delivery confirmation
- Late-arriving data

Do not implement a data quality framework until the requirements are finalized.

---

## 51. Idempotency

Both streaming and batch processing should account for duplicates and retries.

Important patterns may include:

```
event_id
business key
deduplication
upsert
MERGE where appropriate
checkpointing
replay-safe transformations

```

Reprocessing the same source data should not silently produce incorrect duplicate business results.

---

## 52. Late and Out-of-Order Events

Transportation events may arrive late or out of order.

Example:

```
VEHICLE_DEPARTED   10:00
IN_TRANSIT         10:05
DELIVERY_DELAYED   10:30

but Kafka receives IN_TRANSIT after DELIVERY_DELAYED.

```

The architecture must account for this.

This is especially important for Flink event-time processing.

---

## 53. Schema Evolution

Streaming event serialization and schema evolution must use JSON-based event contracts.

Every production-style event should include an explicit schema/version indicator, for example:

```json
{
  "schema_version": 1,
  "event_id": "...",
  "event_type": "...",
  "event_timestamp": "...",
  "payload": {}
}
```

JSON evolution rules:

- Prefer backward-compatible additive changes.
- New optional fields are allowed when existing consumers can safely ignore them.
- Do not silently rename or remove existing fields.
- Do not change the semantic meaning or data type of an existing field without a versioned migration.
- Breaking changes require an explicit new schema version and consumer migration strategy.
- Required vs optional fields must be documented.
- Unknown fields should be handled deliberately by consumers.
- Producers and consumers must validate mandatory identifiers and timestamps.
- Replay of older JSON schema versions must remain possible where business requirements require it.
- Schema evolution tests should cover compatibility between supported producer and consumer versions.

Analytical schema evolution is handled through Apache Iceberg capabilities where appropriate, with approved rules for adding, renaming, deleting, or changing columns and for partition evolution.

The architecture must maintain clear JSON event contracts even though a separate external schema registry is not required for the initial local implementation.

---

## 54. Replayability

Kafka and Data Lake design should make it possible to replay or reprocess events when necessary.

Use cases:

- Bug fix
- New transformation
- Backfill
- KPI recalculation
- Incident recovery
- Data quality correction

---

## 55. Observability Requirements

Future observability should cover:

- Container health
- Kafka broker health
- Kafka consumer lag
- Event throughput
- Failed events
- Flink processing latency
- Flink checkpoint status
- Flink backpressure
- Spark job duration
- Spark failures
- Airflow scheduler health
- Airflow DAG run status
- Airflow task failures and retries
- Airflow scheduling delay where relevant
- PostgreSQL availability
- PostgreSQL connection usage where relevant
- Data Lake write failures
- ClickHouse availability
- ClickHouse ingestion latency
- ClickHouse query latency
- Data freshness
- Invalid event count
- Delayed delivery count

Prometheus/Grafana may be considered later but must not be introduced automatically.

---

## 56. Testing Requirements

Possible test layers:

```
Unit tests
Integration tests
Schema contract tests
Kafka producer/consumer tests
Flink transformation tests
Spark transformation tests
Airflow DAG import/validation tests
Airflow-to-Spark orchestration tests
PostgreSQL integration tests where PostgreSQL is used by the workflow
Data quality tests
Docker health tests
End-to-end tests

```

Do not automatically implement every testing layer.

Testing strategy should evolve with the platform.

---

## 57. Security Requirements

Even for a local lab, follow sensible security principles.

Consider:

- Do not commit production secrets.
- Clearly mark development credentials.
- Least-privilege mindset.
- Avoid unnecessary host port exposure.
- Avoid Docker socket mounts.
- Separate internal and external endpoints.
- Support future TLS.
- Support future secret manager integration.
- Avoid embedding credentials in source code.

---

## 58. Production Engineering Principles

Design decisions should account for:

- Scalability
- Fault tolerance
- Idempotency
- Replayability
- Data contracts
- Schema evolution
- Data quality
- Observability
- Structured logging
- Metrics
- Health checks
- Security
- Configuration management
- Network isolation
- Retry strategies
- Dead-letter handling
- State recovery
- Backpressure
- Checkpointing
- Testing
- Maintainability
- Cost optimization

However, local development must remain understandable and manageable.

Do not overengineer the project.

---

## 59. Explicit Non-Goals for Initial Implementation

Do not automatically introduce:

```
Kubernetes
Terraform
Helm
Service Mesh
Multiple metadata catalogs
Complex CI/CD
Large microservice architecture
Machine learning infrastructure
Optimization solvers
Multiple databases solving the same purpose
Complex security infrastructure

```

These may be introduced later only if justified.

---

## 60. Architecture Decision Rules

For significant architectural decisions, present alternatives instead of silently selecting one.

The following architectural choices are already approved and must not be reopened unless the user explicitly requests a change:

```
Streaming serialization / event contracts -> JSON
Data Lake table format                  -> Apache Iceberg
Iceberg catalog                         -> Nessie
Airflow executor                        -> LocalExecutor
BI platform                             -> Superset

```

Examples of decisions that still require explicit design/approval where not yet finalized:

```
ClickHouse table/serving design
PyFlink vs Java Flink
Flink SQL vs DataStream API
PySpark vs Scala
PostgreSQL database/role isolation strategy
Single Kafka topic vs domain topics
S3-compatible object-storage implementation
Iceberg partitioning and maintenance strategy
Nessie namespace/reference strategy
Superset semantic model and refresh approach

```

For each important decision provide:

```
Problem
Option A
Option B
Advantages
Disadvantages
Operational impact
Recommendation

```

Then obtain explicit user approval.

---

## 61. Strict Codex Approval Workflow

This requirement is mandatory for the entire project.

Codex must NEVER autonomously move through an implementation sequence.

Before every meaningful implementation action:

1. Explain the current state.
2. Recommend exactly one next step.
3. Explain why.
4. Identify files/services affected.
5. Explain risk.
6. Ask for approval.

Use approximately:

```
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

Approval required:
Do you want me to proceed with this exact step?

```

---

## 62. Approval Scope

Approval applies only to the immediately proposed action.

If the user says:

```
yes
continue
proceed
ok
do it

```

this approves only the exact preceding action.

It is NOT permission to continue with subsequent actions.

---

## 63. File Creation Rules

Before creating a new file after `requirement.md`, Codex must describe:

```
File:
<path>

Purpose:
...

Proposed design:
...

Do you approve creating this file?

```

Wait for approval.

---

## 64. Existing File Modification Rules

Never overwrite an existing file blindly.

Before modifying it:

1. Inspect it.
2. Explain its current role.
3. List exact proposed changes.
4. Explain the reason.
5. Ask for approval.

Approval is limited to those stated modifications.

Do not perform unrelated cleanup.

---

## 65. Command Execution Rules

Before executing a command, show:

```
Command:
<command>

Purpose:
...

Impact:
Read-only / Non-destructive / Changes files / Starts services / Destructive

Expected result:
...

Do you approve running this command?

```

Wait for approval.

---

## 66. Failure Handling

If an approved operation fails:

1. Stop.
2. Show the relevant error.
3. Explain the likely cause.
4. Propose one fix.
5. Ask for approval.

Do not enter an automatic trial-and-error loop.

---

## 67. Out-of-Scope Issues

If Codex discovers another problem while performing an approved task:

```
I noticed an additional issue:

...

It is outside the currently approved scope.

I have not changed it.

```

Do not silently fix it.

---

## 68. Destructive Operations

Any operation that can delete or overwrite persistent data requires explicit confirmation immediately before execution.

Examples:

```
DELETE
DROP
TRUNCATE
rm
rm -rf
docker compose down -v
docker volume rm
docker system prune
Kafka topic deletion
Bucket deletion
Database reset
Data Lake reset
File overwrite

```

Clearly state what will be lost.

---

## 69. Implementation Philosophy

The project should evolve incrementally.

A possible high-level roadmap may be:

```
Phase 1  - Repository foundation
Phase 2  - Docker/environment foundation
Phase 3  - Kafka
Phase 4  - Event producer
Phase 5  - Flink
Phase 6  - ClickHouse serving
Phase 7  - Data Lake
Phase 8  - Airflow orchestration and Spark batch
Phase 9  - Analytical serving / Superset
Phase 10 - Testing and observability

```

This roadmap is informational only.

Never execute multiple phases automatically.

---

## 70. Definition of Success

The local platform should eventually allow a Data Engineer to:

```
Start the platform
Generate logistics events
Publish events to Kafka
Process them with Flink
Observe current delivery state
Persist historical/raw information
Schedule and orchestrate daily workflows with Airflow
Process daily data with Spark
Use PostgreSQL safely for approved relational and Airflow metadata workloads
Calculate transportation KPIs
Query analytical results
Visualize results in Superset
Stop and restart the environment safely
Replay historical events
Debug individual services

```

The platform should demonstrate a realistic end-to-end modern Data Engineering architecture for transportation and delivery operations.

---

## 71. Final Developer Experience

The desired end-state should provide a simple workflow similar to:

```
make help
make up
make health
make status
make console

make logs-kafka
make logs-flink
make logs-spark
make logs-airflow
make logs-postgres

make restart
make down

```

Infrastructure should eventually be managed through:

```
Docker Compose
Makefile
Environment variables
Service configuration
Shared Docker networking
Named volumes
Health checks
Internal/external endpoint configuration

```

---

## 72. Final Mandatory Rule

The repository should be built using this principle:

> Think proactively about architecture, performance, production engineering, and the next logical step, but never perform the next meaningful action without explicit user approval.

For this current task, however, creation of **`requirement.md`** **itself is already explicitly approved**.

Create only:

```
requirement.md

```

Populate it with the complete specification above.

Do not perform any other project modification.

After creating it, stop and report that the requirement document has been created.
