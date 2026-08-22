# DeliveryFlow — Scalability & Multi-Application Architecture Prompts

Bu sənəd DeliveryFlow layihəsi üçün son hazırlanmış iki professional promptu bir yerdə saxlayır:

1. **300–500 Table və Multi-Application Scale Architecture Review**
2. **Second Streaming Application with Java + Flink SQL**

---

# 1. DeliveryFlow — 300–500 Table və Multi-Application Scale Architecture Review

Sən **Principal Data Platform Architect, Staff Data Engineer, Streaming Architect, Lakehouse Architect, ClickHouse Architect, Apache Kafka/Flink/Spark Engineer və Platform Engineering Lead** kimi fəaliyyət göstər.

Mənim mövcud DeliveryFlow layihəm aşağıdakı arxitekturaya malikdir:

```text
Streaming:
Producer -> Kafka -> Flink -> ClickHouse -> Superset

Batch:
PostgreSQL / ClickHouse -> Spark -> Iceberg + Nessie + MinIO -> ClickHouse -> Superset

Orchestration:
Airflow -> Spark

BI:
ClickHouse -> Superset
```

Mövcud əsas texnologiyalar:

```text
Kafka 3.9.2
Flink 1.20.3
Spark 3.5.6
Iceberg 1.11.0
Nessie 0.107.5
Airflow 2.10.5
Superset 4.1.2
PostgreSQL 16
ClickHouse 25.8
MinIO
Docker Compose
Python
Java Flink DataStream
```

Hazırkı layihə bir əsas logistics application və az sayda cədvəl üçün qurulub.

Mən platformanı gələcəkdə aşağıdakı scale üçün hazırlamaq istəyirəm:

```text
10+ application / business domain

300–500+ source və target table

çoxlu PostgreSQL / DB source

çoxlu ETL application

çoxlu Spark batch job

çoxlu Kafka event domain

çoxlu Flink streaming workload

çoxlu Iceberg table

çoxlu ClickHouse serving table/view

100+ Superset dataset/chart/dashboard

fərqli schedule və SLA-lar

incremental load

full load

CDC

backfill

reprocessing
```

Məqsədim indidən bütün sistemi unnecessarily distributed etmək deyil.

Əsas məqsəd:

> Mövcud sadə DeliveryFlow platformasını elə refactor etməkdir ki, sabah 300–500 table və daha çox ETL application əlavə olunanda hər table üçün ayrıca custom Python script, ayrıca DAG, ayrıca Spark app, ayrıca config və manual deployment yaratmağa ehtiyac qalmasın.

## 1.1. Əvvəlcə mövcud arxitekturanı qiymətləndir

Hazırkı qərarlar bunlardır:

### Kafka

```text
1 KRaft broker
1 əsas topic: delivery-events
key = delivery_id
```

### Flink

Bir Java DataStream application:

```text
Kafka
  ->
JSON parse
  ->
schema_version validation
  ->
delivery state
  ->
vehicle state
  ->
ClickHouse
```

### ClickHouse

Hazırda həm:

```text
Real-time operational serving
+
BI serving
```

rolunu oynayır.

Əsas obyektlər:

```text
delivery.delivery_events
delivery.delivery_current_state
delivery.vehicle_current_state
delivery.daily_delivery_kpi

BI views
```

### Spark

Hazırda əsas batch application:

```text
src/etl/apps/daily_kpi_job.py
```

və aşağıdakı flow:

```text
ClickHouse
   ->
Spark
   ->
Iceberg
   ->
ClickHouse serving tables
```

### Iceberg / Nessie / MinIO

```text
Iceberg = analytical table format
Nessie = catalog
MinIO = object storage
```

Hazırda nümunə table-lar:

```text
nessie.bronze.raw_delivery_events
nessie.gold.daily_delivery_kpi
```

### Airflow

Hazırda:

```text
LocalExecutor
Airflow -> spark-submit
```

modelindən istifadə olunur.

### PostgreSQL

İki service:

```text
postgres-airflow
postgres-source
```

`postgres-source` daxilində:

```text
logistics_source
nessie_metadata
```

database-ləri mövcuddur.

### Superset

Approved BI source:

```text
ClickHouse
```

BI obyektləri BI-as-Code kimi YAML-dan import olunur.

Hazırda:

```text
configs/superset/deliveryflow_bi.yaml
```

database, dataset, chart və dashboard definition-larını saxlayır.

## 1.2. Əsas sual

Bu architecture 300–500 table və çoxlu ETL application olduqda hansı nöqtələrdə problem yaradacaq?

Mənə sadəcə:

```text
Kafka-nı scale et
Spark worker artır
ClickHouse cluster qur
```

kimi generic cavab vermə.

Əsas architecture problemini tap.

Xüsusilə aşağıdakı anti-pattern-ləri müəyyən et:

```text
one Python file per table

one Spark job per table

one Airflow DAG per table

one Flink job per table

one Kafka topic per table

one Superset YAML for everything

one .env containing every table configuration

hardcoded schemas

hardcoded table names

hardcoded source/target mappings

manual onboarding

application-specific logic mixed with platform logic
```

Hər bir problem üçün göstər:

```text
Current design

Scale zamanı problem

Nə üçün problem yaranacaq

Recommended architecture

Trade-off

Nə vaxt tətbiq edilməlidir
```

## 1.3. Əsas target: Metadata-Driven Data Platform

Ən ciddi şəkildə bu yanaşmanı analiz et:

> 300–500 table üçün pipeline logic table-specific deyil, metadata/config-driven olmalıdır.

Məsələn yeni table əlavə etmək üçün ideal halda yeni Python ETL yazmaq əvəzinə yalnız metadata yaradılmalıdır.

Conceptual example:

```yaml
application: logistics
domain: delivery

source:
  type: postgres
  connection: logistics_source
  schema: public
  table: customer_orders

ingestion:
  mode: incremental
  primary_key:
    - order_id
  watermark_column: updated_at

target:
  layer: bronze
  table: logistics.customer_orders
  format: iceberg

schedule:
  cron: "0 1 * * *"

quality:
  not_null:
    - order_id
    - updated_at

  uniqueness:
    - order_id

owner:
  team: data-platform

sla:
  freshness_minutes: 120
```

Bu sadəcə conceptual example-dır.

Mənim platformama uyğun professional metadata model təklif et.

Metadata aşağıdakıları idarə edə bilməlidir:

```text
application
domain
source system
source schema
source table
target table
load type
full load
incremental load
CDC
primary key
watermark
partition
schedule
SLA
owner
quality rules
retention
criticality
stream/batch
serving requirement
BI exposure
```

## 1.4. Control Plane və Data Plane dizaynı

Platformanı aşağıdakı iki logical hissəyə bölməyin lazım olub-olmadığını analiz et:

```text
CONTROL PLANE
--------------------------------
Application Registry
Dataset Registry
Pipeline Metadata
Schemas / Contracts
Scheduling Metadata
Data Quality Rules
Ownership
SLA
Configuration
Deployment metadata


DATA PLANE
--------------------------------
Kafka
Flink
Spark
Airflow
Iceberg
MinIO
ClickHouse
Superset
```

Target flow:

```text
                    CONTROL PLANE

             Application Registry
                     |
              Dataset Registry
                     |
             Pipeline Metadata
                     |
        +------------+-------------+
        |            |             |
     Contracts    Quality        Ownership/SLA
        |
        v

                     DATA PLANE

Sources
   |
   +-------------------------+
   |                         |
   v                         v
Streaming                 Batch / CDC
   |                         |
Kafka                   Generic Ingestion
   |                         |
Flink                      Spark
   |                         |
   +-----------+-------------+
               |
               v
        Iceberg / MinIO
               |
        +------+------+
        |             |
        v             v
    ClickHouse      Lakehouse
        |
        v
    Superset / BI
```

Mənə de:

- Control Plane həqiqətən lazımdırmı?
- Hansı metadata orada olmalıdır?
- Metadata YAML-də olmalıdır?
- PostgreSQL-də olmalıdır?
- Git-də olmalıdır?
- Hybrid model daha yaxşıdırmı?
- Runtime həmin metadata-nı necə oxumalıdır?

Overengineering etmə.

## 1.5. Platform Logic və Application Logic ayrılmalıdır

Hazırkı repository-ni aşağıdakı prinsipə uyğun necə refactor etməyi təklif et:

```text
Shared Platform Logic
        !=
Application-Specific Logic
```

Məsələn architecture buna yaxın ola bilər:

```text
platform/
├── ingestion/
├── streaming/
├── batch/
├── lakehouse/
├── serving/
├── quality/
└── common/

apps/
├── delivery/
│   ├── contracts/
│   ├── pipelines/
│   ├── tables/
│   ├── quality/
│   └── bi/
│
├── fleet/
│   ├── contracts/
│   ├── pipelines/
│   ├── tables/
│   └── bi/
│
├── warehouse/
│   ├── contracts/
│   ├── pipelines/
│   └── bi/
│
└── finance/
```

Bu structure-u kor-koranə qəbul etmə.

Mövcud DeliveryFlow repository-sinə uyğun daha professional repository architecture təklif et.

## 1.6. Application Registry

Yeni application onboarding üçün ayrıca registry pattern lazım olub-olmadığını analiz et.

Məsələn:

```yaml
application: delivery

owner: logistics-team

streaming:
  enabled: true

batch:
  enabled: true

serving:
  clickhouse: true

bi:
  superset: true
```

Sonra:

```text
delivery
fleet
warehouse
orders
customer
finance
inventory
```

kimi application-lar platformaya register edilə bilsin.

Application isolation architecture təklif et.

## 1.7. Dataset Registry

300–500 table üçün centralized dataset metadata model təklif et.

Məsələn:

```text
dataset_id
application
domain
source
source_table
target
target_table
load_mode
primary_key
watermark
schedule
owner
SLA
quality
retention
```

Yeni table onboarding maksimum dərəcədə config-driven olmalıdır.

## 1.8. Kafka Architecture

Hazırkı:

```text
delivery-events
```

topic strategiyasını multi-application architecture üçün analiz et.

Aşağıdakıları müqayisə et:

```text
Topic per table

Topic per entity

Topic per domain

Topic per event family

Shared event topic
```

300–500 table olması 300–500 Kafka topic demək deyil.

Xüsusilə:

```text
topic explosion
partition explosion
broker metadata
consumer groups
ordering
failure isolation
retention
```

risklərini nəzərə al.

Professional naming convention təklif et.

Məsələn:

```text
<environment>.<domain>.<entity>.<event-category>
```

amma daha yaxşı variant varsa onu seç.

Ayrıca göstər:

```text
Local:
1 broker

Production:
minimum realistic HA topology
```

və production scale-out hansı metric-lərə əsasən edilməlidir.

## 1.9. Schema Registry ehtiyacını analiz et

Hazırda JSON + `schema_version = 1` istifadə olunur.

Multi-application environment üçün aşağıdakı variantları müqayisə et:

```text
JSON Schema in Git

Kafka Schema Registry + Avro

Kafka Schema Registry + Protobuf

Hybrid
```

Nəzərə al:

```text
schema compatibility
versioning
breaking changes
producer/consumer contract
multi-team development
CI validation
operational complexity
```

Sadəcə enterprise görünsün deyə Schema Registry əlavə etmə.

Nə vaxt lazım olduğunu konkret de.

## 1.10. Flink Architecture

Ən ciddi qərarlardan biridir.

Hazırda bir DeliveryFlow Flink job var.

300–500 table və çoxlu application üçün müqayisə et:

```text
One giant Flink job

One Flink job per table

One Flink job per application

One Flink job per domain

One Flink job per business capability
```

Hər variant üçün analiz et:

```text
failure blast radius
deployment isolation
checkpoint size
state size
parallelism
task slots
backpressure
upgrade
savepoint
ownership
SLA
resource usage
```

Məqsəd:

```text
500 table = 500 always-running Flink jobs
```

kimi model yaratmamaqdır.

Optimal grouping strategy təklif et.

## 1.11. Generic Streaming Framework

Ortaq streaming logic varsa onu reusable etmək üçün necə architecture qurulmalıdır?

Məsələn:

```text
Kafka Source
     |
     v
Generic Validation
     |
     v
Schema / Contract
     |
     v
Routing
     |
     +----------+
     |          |
     v          v
Domain Job    Domain Job
```

Nəyi generic etmək lazımdır və nəyi application-specific saxlamaq lazımdır?

## 1.12. Spark Architecture

Hazırkı:

```text
daily_kpi_job.py
```

yanaşması az sayda job üçün yaxşıdır.

Amma 300–500 table üçün aşağıdakıları müqayisə et:

```text
One Spark job per table

One giant Spark job

One Spark job per application

One Spark job per domain

Metadata-driven reusable Spark ingestion framework
```

Target Spark framework aşağıdakıları support etməlidir:

```text
full load
incremental load
watermark
CDC
merge/upsert
partition pruning
parallel table processing
backfill
retry
reprocessing
schema evolution
Iceberg write
ClickHouse publish
```

Yeni table üçün Python code yazmaq əvəzinə metadata dəyişməsinin kifayət edib-etməyəcəyini analiz et.

## 1.13. ETL Application Architecture

ETL application sayı artdıqda aşağıdakı structure-u analiz et:

```text
src/etl/
├── framework/
│   ├── readers/
│   ├── writers/
│   ├── transforms/
│   ├── validation/
│   └── metadata/
│
└── apps/
    ├── delivery/
    ├── fleet/
    └── warehouse/
```

Generic component-lər:

```text
PostgresReader
ClickHouseReader
IcebergWriter
ClickHouseWriter
IncrementalLoader
FullLoader
MergeLoader
QualityValidator
```

kimi abstraction-lar faydalı ola bilər.

Amma unnecessary object-oriented abstraction yaratma.

Reusable interface-lərlə sadə architecture təklif et.

## 1.14. Airflow Architecture

300–500 table üçün:

```text
500 manually written DAG
```

qəbul edilən solution deyil.

Aşağıdakı variantları müqayisə et:

```text
DAG Factory

Dynamic Task Mapping

One DAG per domain

One DAG per application

Metadata-driven DAG

Dataset-aware scheduling
```

Target belə ola bilər:

```text
Metadata
   |
   v
Airflow DAG
   |
   +--> Table A
   +--> Table B
   +--> Table C
   +--> Table D
```

və ya application-level workflow.

Airflow üçün də failure isolation, pools və concurrency nəzərə al:

```text
application pool
source system pool
Spark concurrency
DB connection limits
```

## 1.15. Dependency Graph

Table-lar arasında dependency varsa metadata-driven dependency model təklif et.

Məsələn:

```text
bronze.orders
       |
       v
silver.orders
       |
       +----------------+
       |                |
       v                v
gold.sales        gold.delivery
```

Airflow bunu necə idarə etməlidir?

Hardcoded Python dependency-lərdən qaçmaq üçün yanaşma təklif et.

## 1.16. Iceberg Architecture

300–500 Iceberg table normal scale ola bilər.

Əsas problem table sayı deyil, metadata və file management-dir.

Aşağıdakıları analiz et:

```text
namespace strategy
partitioning
small files
file sizing
snapshot growth
manifest growth
metadata growth
compaction
snapshot expiration
orphan cleanup
schema evolution
partition evolution
```

Application/domain-based namespace standardı təklif et.

## 1.17. MinIO / Object Storage Layout

300–500 table üçün:

```text
one bucket per table
```

yaratma.

Daha professional hierarchy təklif et.

Məsələn:

```text
warehouse/
├── delivery/
├── fleet/
├── warehouse/
└── finance/
```

və ya Iceberg-in catalog-managed layout-u.

Nəzərə al:

```text
object count
prefixes
lifecycle
retention
backup
production object storage migration
```

## 1.18. Nessie Architecture

Nessie yüzlərlə table üçün necə istifadə edilməlidir?

Analiz et:

```text
namespace strategy
catalog availability
PostgreSQL backend
commit concurrency
branch usage
backup
metadata lifecycle
```

Nessie branch/tag functionality-ni hər ETL run üçün istifadə etmək lazımdırmı?

Yoxsa yalnız xüsusi release/data promotion use case-lərində?

Overuse etmə.

## 1.19. PostgreSQL Separation

Hazırda:

```text
postgres-airflow
postgres-source
```

var.

`postgres-source` daxilində:

```text
logistics_source
nessie_metadata
```

var.

Production architecture üçün aşağıdakı separation lazım olub-olmadığını analiz et:

```text
Airflow Metadata DB

Nessie Metadata DB

Control Plane Metadata DB

Business Source PostgreSQL
```

Local lab-da neçə PostgreSQL container saxlamaq məqsədəuyğundur?

Production-da failure domain və ownership necə ayrılmalıdır?

## 1.20. ClickHouse Architecture

ClickHouse hazırda:

```text
Operational serving
+
BI serving
+
Spark source
```

kimi istifadə olunur.

Scale zamanı aşağıdakı problemləri nəzərə al:

```text
streaming writes
batch writes
BI queries
Spark reads
materialized views
aggregation
concurrent users
query memory
MergeTree background merges
mutations
TTL
large scans
```

Aşağıdakı qərarları qiymətləndir:

```text
one ClickHouse database per application?

one shared database with namespaces?

separate operational and BI schema?

same cluster with resource isolation?

separate cluster only when scale requires?
```

Table engine design üçün də:

```text
MergeTree
ReplacingMergeTree
AggregatingMergeTree
SummingMergeTree
Materialized Views
Projections
```

nə zaman istifadə olunmalıdır izah et.

Amma 500 table olduğuna görə avtomatik sharding yaratma.

## 1.21. ClickHouse Scale Triggers

Mənə konkret symptoms göstər:

```text
BI query latency increases while streaming inserts remain high

background merges constantly saturated

memory contention

Spark JDBC reads affect BI

high insert concurrency

single node cannot meet SLA
```

Bu symptoms olduqda hansı architectural action görülməlidir?

## 1.22. Superset BI-as-Code Architecture

Hazırda:

```text
configs/superset/deliveryflow_bi.yaml
```

bir file-də database, dataset, chart və dashboard definition-ları saxlayır.

Bu 5 dataset üçün normaldır.

Amma 100+ dataset və çoxlu application üçün monolithic YAML problem yaradacaq.

Target modular BI-as-Code structure təklif et.

Məsələn:

```text
configs/superset/
├── delivery/
│   ├── datasets/
│   ├── charts/
│   └── dashboards/
│
├── fleet/
│   ├── datasets/
│   ├── charts/
│   └── dashboards/
│
└── warehouse/
```

Aşağıdakıları dizayn et:

```text
stable UUID
dependency resolution
environment-independent definitions
dataset validation
chart validation
dashboard validation
idempotent import
domain-scoped import
rollback
Git review
```

Developer experience məsələn belə ola bilər:

```bash
make superset-import APP=delivery
make superset-import APP=fleet
```

Bu yanaşmanın dəyərini qiymətləndir.

## 1.23. Configuration Architecture

Hazırda `.env` global local config saxlayır.

300–500 table üçün hər table config-i `.env` daxilində saxlamaq olmaz.

Config hierarchy təklif et:

```text
Platform configuration

Environment configuration

Application configuration

Dataset/Table configuration

Secrets
```

Məsələn:

```text
.env

configs/
├── platform/
├── applications/
├── datasets/
├── quality/
└── contracts/
```

Nəyin `.env`-də, nəyin YAML-də, nəyin secret store-da olmalı olduğunu dəqiq izah et.

## 1.24. Secrets və Configuration ayrımı

Bu ikisini qarışdırma:

```text
CONFIGURATION
source table
target table
load mode
schedule
watermark

SECRET
username
password
token
secret key
```

Local və production strategy-ni ayrıca göstər.

## 1.25. Data Contract Architecture

Hazırda:

```text
delivery_event_v1.schema.json
```

kimi contract modeli istifadə olunur.

Multi-application environment üçün structure təklif et:

```text
contracts/
├── delivery/
│   ├── delivery_event_v1.schema.json
│   └── vehicle_event_v1.schema.json
│
├── fleet/
└── warehouse/
```

Schema evolution policy təklif et:

```text
backward compatible
forward compatible
breaking change
version bump
deprecation
```

## 1.26. Data Quality Architecture

300–500 table üçün quality rules hardcoded Python olmamalıdır.

Metadata-driven quality model təklif et.

Məsələn:

```yaml
quality:
  not_null:
    - order_id
    - created_at

  unique:
    - order_id

  freshness:
    column: updated_at
    max_minutes: 120
```

Platform generic validator istifadə edə bilər.

Aşağıdakıları nəzərə al:

```text
not null
uniqueness
accepted values
freshness
referential integrity
row count
volume anomaly
schema drift
```

## 1.27. Observability Architecture

Hazırkı:

```text
make health
logs
smoke test
```

local lab üçün yaxşıdır.

Amma scale üçün metrics aşağıdakı dimension-larla izlənməlidir:

```text
application
dataset
pipeline
job
topic
consumer group
source
target
```

Nəzərə al:

```text
Kafka consumer lag
Flink checkpoint duration
Flink checkpoint failure
Flink backpressure
Spark duration
Spark failure
Airflow queue time
dataset freshness
rows read
rows written
ClickHouse insert latency
ClickHouse query latency
Iceberg file count
Iceberg snapshot count
data quality failures
```

## 1.28. Ownership və Governance

300–500 table üçün hər dataset-in owner-i olmalıdır.

Metadata-da aşağıdakı field-ləri nəzərə al:

```text
owner
application
domain
description
criticality
SLA
PII
retention
source
primary key
freshness
```

## 1.29. Naming Convention

Aşağıdakılar üçün standard yarat:

```text
Kafka topic
Kafka consumer group
Flink job
Spark app
Airflow DAG
Airflow task
Iceberg namespace
Iceberg table
ClickHouse database
ClickHouse table
ClickHouse view
Superset dataset
Superset dashboard
config file
application
```

Naming convention scale üçün consistent olmalıdır.

## 1.30. Application Isolation

Bir application-da problem bütün platformanı dayandırmamalıdır.

Nəzərə al:

```text
Kafka consumer isolation
Flink job isolation
Spark resource isolation
Airflow pools
ClickHouse users/quotas
application namespaces
table-specific failure handling
application-specific secrets
```

Bir domain-in ağır workload-u digər domain-in SLA-sına təsir etməməlidir.

## 1.31. Failure Blast Radius

Məsələn:

```text
fleet domain-da Flink backpressure
```

olarsa:

```text
delivery domain
```

dayanmamalıdır.

Eyni prinsip:

```text
Spark
Airflow
ClickHouse
Kafka
```

üçün də tətbiq olunmalıdır.

Target isolation model təklif et.

## 1.32. Table Onboarding Experience

Yeni table əlavə etmək üçün developer aşağıdakıları etməyə məcbur olmamalıdır:

```text
new Python application
new Spark implementation
new Airflow DAG
manual ClickHouse DDL
manual Iceberg creation
manual quality code
manual monitoring
```

Əvəzində ideal onboarding:

```text
1. Metadata əlavə et.
2. Contract əlavə et.
3. Lazım olsa custom transformation əlavə et.
4. Validate et.
5. Deploy et.
```

Target developer experience təklif et.

Məsələn:

```bash
make validate-app APP=delivery
make validate-dataset APP=delivery DATASET=orders
make run-dataset APP=delivery DATASET=orders
make backfill APP=delivery DATASET=orders DATE_FROM=... DATE_TO=...
```

Bu command-ları indi implement etmə.

Architecture baxımından evaluate et.

## 1.33. Generic vs Custom Transformation

Bütün pipeline-ları generic etmək də səhvdir.

Aydın ayır:

### Generic

```text
read
incremental filter
schema validation
quality validation
standard audit columns
write
metrics
retry
```

### Application-specific

```text
business transformation
complex joins
KPI formulas
stateful event logic
```

Bu separation üçün professional code architecture təklif et.

## 1.34. Audit Columns

300–500 table üçün standard operational metadata columns lazım olub-olmadığını analiz et.

Məsələn:

```text
_ingested_at
_source_system
_pipeline_run_id
_batch_id
_schema_version
_source_file
_created_at
_updated_at
```

Streaming və batch üçün hansı audit field-lərin faydalı olduğunu de.

## 1.35. Local vs Production

Local environment sadə qalmalıdır.

Local üçün məsələn:

```text
1 Kafka broker

1 Flink JobManager
1 TaskManager

1 Spark master
1 worker

1 ClickHouse

1 MinIO

1 Nessie

Airflow LocalExecutor
```

qəbul edilə bilər.

300–500 table metadata repository-də olsa belə developer workstation-da hamısı eyni anda run edilməməlidir.

Local profile/application filtering strategiyası təklif et.

Məsələn conceptual:

```bash
make up APP=delivery
```

və ya Compose profile.

Production architecture-ni ayrıca izah et.

## 1.36. Docker Compose Scope

Docker Compose local development orchestration olaraq qalsın.

Production scale üçün Compose-u cluster orchestrator kimi istifadə etməyi təklif etmə.

Production deployment üçün alternativləri yalnız ehtiyac olduğu halda qeyd et:

```text
managed services
Kubernetes
VM-based clusters
cloud-native services
```

Amma bunları indidən implement etmə.

## 1.37. Scale Trigger Matrix

Bu hissə mütləq olsun.

Hər platform component üçün:

```text
Metric / Symptom
Current architecture
Threshold pattern
Recommended action
```

formatında göstər.

Components:

```text
Kafka
Flink
Spark
Airflow
ClickHouse
Iceberg
MinIO
Nessie
PostgreSQL
Superset
```

Fake universal rəqəm vermə.

Əgər exact threshold workload-dan asılıdırsa symptom-based trigger ver.

## 1.38. "İndi dəyişməliyik" və "Sonra dəyişməliyik"

Recommendation-ları üç qrupa ayır.

### A — İndi dəyişməliyik

Gələcək refactor riskini azaldan foundation dəyişiklikləri.

Məsələn potensial:

```text
domain/application organization
metadata-driven dataset registry
generic ETL framework
config separation
naming standards
Superset YAML modularization
Airflow metadata-driven orchestration
ownership metadata
quality metadata
```

### B — Workload artdıqda dəyişməliyik

Runtime scale.

Məsələn:

```text
Kafka brokers
Flink TaskManagers
Spark workers
ClickHouse replicas
MinIO distributed setup
```

### C — Production SLA tələb etdikdə

Məsələn:

```text
HA deployment
Kubernetes
managed Kafka
distributed ClickHouse
central secrets manager
advanced observability
```

Ən vacib məqsəd:

> Production complexity-ni indidən əlavə etmə, amma architecture foundation-u production scale-a mane olmayacaq şəkildə qur.

## 1.39. Migration Strategy

Mövcud DeliveryFlow işləyir.

Big-bang rewrite istəmirəm.

Incremental migration plan ver.

Hər phase üçün göstər:

```text
Goal
What changes
Why
Risk
Backward compatibility
What existing behavior stays
```

## 1.40. Existing Architecture-dan nə qalmalıdır?

Ayrıca göstər:

```text
KEEP AS-IS

REFACTOR NOW

CHANGE LATER

REMOVE
```

kateqoriyaları.

Tool dəyişmək əvəzinə architecture problem-ni həll etməyə üstünlük ver.

## 1.41. Target Repository Structure

Final recommendation-da 300–500 table üçün maintainable repository tree göstər.

Aydın şəkildə ayır:

```text
platform code
application code
dataset metadata
contracts
quality rules
BI definitions
infrastructure
orchestration
tests
documentation
```

## 1.42. Target Architecture Diagram

Sonda yeni architecture-ni Mermaid ilə göstər.

Diaqramda ən azı bunlar görünsün:

```text
Sources
Metadata / Control Plane
Kafka
Flink
Spark
Airflow
Iceberg
Nessie
MinIO
ClickHouse
Superset
Application/domain separation
```

## 1.43. Son cavabın strukturu

Cavabı aşağıdakı ardıcıllıqla ver:

```text
1. Executive Summary
2. Mövcud Architecture-də yaxşı olan hissələr
3. 300–500 Table üçün əsas problemlər
4. Multi-Application üçün əsas problemlər
5. Əsas architectural dəyişiklik
6. Metadata-Driven Platform Design
7. Control Plane vs Data Plane
8. Target Repository Structure
9. Application və Domain Isolation
10. Dataset Registry
11. Kafka Architecture
12. Schema Management
13. Flink Architecture
14. Spark / ETL Framework
15. Airflow Orchestration
16. Iceberg / Nessie / MinIO
17. ClickHouse Serving Architecture
18. PostgreSQL Architecture
19. Superset BI-as-Code
20. Configuration and Secrets
21. Data Quality
22. Observability
23. Governance və Ownership
24. Naming Standards
25. Table Onboarding Workflow
26. Generic vs Custom Logic
27. Local vs Production
28. Scale Trigger Matrix
29. KEEP / REFACTOR NOW / CHANGE LATER / REMOVE
30. What We Should Change Now
31. What We Should NOT Change Yet
32. Incremental Migration Roadmap
33. Final Target Architecture Diagram
```

## 1.44. Ən vacib design prinsipləri

Architecture aşağıdakı prinsiplərə əsaslanmalıdır:

```text
Configuration over duplication
Metadata over hardcoding
Shared platform over table-specific infrastructure
Domain isolation over one giant application
Reusable framework over copy/paste ETL
Horizontal scaling over vertical assumptions
Git-versioned definitions
Clear ownership
Idempotent processing
Incremental processing
Replayability
Observability
Failure isolation
Backward compatibility
```

Amma unutma:

> Metadata-driven architecture qurmaq bütün business logic-i YAML-a çevirmək demək deyil.

Complex business transformation-lar yenə code-da qalmalıdır.

Metadata əsasən orchestration, ingestion, configuration, schema, quality, ownership və operational behavior-u idarə etməlidir.

## 1.45. Final məqsəd

Mən istəyirəm ki, platformada 1 table və ya 500 table olması əsas architecture-nı dəyişməsin.

Yeni application onboarding belə düşünülməlidir:

```text
Register Application
       |
       v
Register Datasets
       |
       v
Contracts + Config
       |
       v
Generic Platform
       |
   +---+-----------------------+
   |                           |
   v                           v
Streaming                    Batch
Kafka/Flink                Airflow/Spark
   |                           |
   +-------------+-------------+
                 |
                 v
         Iceberg / MinIO
                 |
                 v
            ClickHouse
                 |
                 v
          Superset BI
```

Table sayı artdıqca əsas dəyişiklik:

```text
more metadata
more compute
more storage
```

olmalıdır.

Bu olmamalıdır:

```text
more duplicated code
more duplicated DAGs
more custom infrastructure
more manual steps
```

Bu mərhələdə **heç bir implementation etmə**.

Mənə əvvəlcə tam architecture review, konkret target design və mərhələli migration roadmap ver.

Mövcud tool stack-i yalnız real technical səbəb olduqda dəyişmə.

---

# 2. DeliveryFlow — Second Streaming Application with Java + Flink SQL

Mövcud DeliveryFlow architecture və əvvəlki scalability qaydalarını saxla.

Əlavə olaraq platformada **ikinci müstəqil streaming application** yaratmaq istəyirəm.

Məqsəd odur ki, hazırkı Java Flink DataStream application-la yanaşı ikinci application **Java + Flink SQL / Table API** istifadə etsin və platformanın multi-application modelini real şəkildə nümayiş etdirsin.

Bu ikinci application sadəcə mövcud `delivery-events` stream-in copy-si olmamalıdır.

Yeni business use case, yeni event contract, yeni Kafka topic və yeni Flink SQL processing flow yaradılmalıdır.

## 2.1. Mövcud Streaming Application

Hazırkı Application 1 aşağıdakı modeldədir:

```text
Application 1 — Delivery Monitoring

Synthetic Producer
        |
        v
Kafka
delivery-events
        |
        v
Java Flink DataStream API
        |
        +--> delivery_events
        |
        +--> delivery_current_state
        |
        +--> vehicle_current_state
        |
        v
ClickHouse
```

Application 1 olduğu kimi qalmalıdır.

Mövcud işləyən implementation-u səbəbsiz refactor və ya rewrite etmə.

Application 2 onun yanında ayrıca işləməlidir.

## 2.2. Yeni Application 2

Yeni application üçün business scenario seç:

> **Fleet Vehicle Telemetry & Vehicle Health Monitoring**

Application adı professional naming convention ilə məsələn:

```text
fleet-monitoring
```

və ya repository naming standardına uyğun daha yaxşı ad ola bilər.

Final naming-i mövcud repository naming convention-a əsasən müəyyən et.

## 2.3. Yeni Event Domain

Yeni event:

```text
VEHICLE_TELEMETRY
```

Əlavə event type-lar lazım olsa aşağıdakılar nəzərə alına bilər:

```text
VEHICLE_TELEMETRY
VEHICLE_ENGINE_WARNING
VEHICLE_FUEL_LOW
VEHICLE_OVERSPEED
VEHICLE_IDLE
```

Amma ilkin implementation-u lazımsız mürəkkəbləşdirmə.

Ən azı bir əsas telemetry event contract yarat.

## 2.4. Vehicle Telemetry Event Contract

Potential contract:

```json
{
  "event_id": "uuid",
  "event_type": "VEHICLE_TELEMETRY",
  "schema_version": 1,
  "event_timestamp": "2026-08-22T10:15:30Z",
  "ingestion_timestamp": "2026-08-22T10:15:31Z",
  "vehicle_id": "VEH-001",
  "driver_id": "DRV-001",
  "latitude": 40.4093,
  "longitude": 49.8671,
  "speed_kmh": 74.5,
  "fuel_level_pct": 63.2,
  "engine_temperature_c": 91.4,
  "odometer_km": 145230.7,
  "engine_status": "RUNNING",
  "vehicle_status": "IN_TRANSIT"
}
```

Bu yalnız conceptual schema-dır.

Implementation etməzdən əvvəl:

- datatype-ları;
- required field-ləri;
- nullable field-ləri;
- validation qaydalarını;
- event key-i;
- partition key-i;

analiz et.

Final schema-nı təklif et və approval al.

## 2.5. Kafka Topic

Application 2 üçün ayrıca Kafka topic istifadə et.

Conceptual:

```text
vehicle-telemetry-events
```

Əsas prinsip:

```text
Application 1:
delivery-events

Application 2:
vehicle-telemetry-events
```

kimi logical isolation olmalıdır.

## 2.6. Kafka Key

Vehicle telemetry event üçün Kafka key:

```text
vehicle_id
```

olmasını evaluate et.

Bu seçim aşağıdakı səbəblərlə uyğun ola bilər:

```text
same vehicle events -> same partition
per-vehicle ordering
Flink GROUP BY / keyed processing
latest vehicle telemetry state
```

Əgər daha uyğun key varsa izah et.

## 2.7. Producer

Mövcud synthetic producer architecture-ni inspect et.

Variantları qiymətləndir:

```text
Option A
Existing producer-i extend etmək

Option B
Fleet-specific producer module yaratmaq

Option C
Shared producer framework + domain generators
```

Multi-application architecture üçün ən maintainable variantı seç.

## 2.8. Flink Implementation — Java + Flink SQL

Application 2 mütləq:

```text
Java
+
Flink Table API / Flink SQL
```

ilə yazılmalıdır.

Application 1-də istifadə olunan DataStream API-ni copy etmə.

Məqsəd:

```text
Application 1
Java DataStream API

Application 2
Java Flink SQL / Table API
```

## 2.9. Target Flink SQL Flow

```text
Kafka
vehicle-telemetry-events
        |
        v
Flink SQL Kafka Source Table
        |
        v
Schema / Event-Time Processing
        |
        v
SQL Transformations
        |
        +----------------------+
        |                      |
        v                      v
Vehicle Latest State     Fleet Metrics
        |                      |
        +----------+-----------+
                   |
                   v
               ClickHouse
```

## 2.10. Flink SQL Source Table

Java application daxilində `TableEnvironment` və ya uyğun `StreamTableEnvironment` istifadə et.

Conceptual example:

```sql
CREATE TABLE vehicle_telemetry_source
(
    event_id STRING,
    event_type STRING,
    schema_version INT,
    event_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    vehicle_id STRING,
    driver_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kmh DOUBLE,
    fuel_level_pct DOUBLE,
    engine_temperature_c DOUBLE,
    odometer_km DOUBLE,
    engine_status STRING,
    vehicle_status STRING,
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND
)
WITH
(
    'connector' = 'kafka',
    ...
);
```

Actual Kafka connector version və Flink 1.20.3 compatibility-ni mövcud pinned dependency set ilə yoxla.

## 2.11. Event Time

Application 2 event-time processing istifadə etməlidir.

Mütləq evaluate et:

```text
event_timestamp
watermark
late event tolerance
out-of-order telemetry events
```

Random interval seçmə.

Reasoning-i sənədləşdir.

## 2.12. Flink SQL Use Cases

İkinci application yalnız Kafka -> ClickHouse copy etməsin.

Ən azı uyğun use case-lərdən istifadə et:

### A. Latest Vehicle Telemetry

```text
vehicle_id
last_event_timestamp
latitude
longitude
speed_kmh
fuel_level_pct
engine_temperature_c
engine_status
vehicle_status
```

### B. Overspeed Detection

```text
speed_kmh > configured threshold
```

### C. Low Fuel Detection

```text
fuel_level_pct < threshold
```

### D. Engine Temperature Warning

```text
engine_temperature_c > threshold
```

### E. Windowed Fleet Metrics

Məsələn tumbling window:

```text
AVG(speed_kmh)
AVG(fuel_level_pct)
MAX(engine_temperature_c)
COUNT(DISTINCT vehicle_id)
```

## 2.13. SQL-Based Alert Classification

Conceptual:

```sql
SELECT
    vehicle_id,
    event_timestamp,
    speed_kmh,
    fuel_level_pct,
    engine_temperature_c,
    CASE
        WHEN engine_temperature_c >= <threshold> THEN 'ENGINE_TEMPERATURE_HIGH'
        WHEN fuel_level_pct <= <threshold> THEN 'LOW_FUEL'
        WHEN speed_kmh >= <threshold> THEN 'OVERSPEED'
        ELSE 'NORMAL'
    END AS health_status
FROM vehicle_telemetry_source;
```

Threshold-ları əvvəl business/config design baxımından müəyyən et.

## 2.14. ClickHouse Target Tables

Application/domain separation principle tətbiq et.

Potential:

```text
fleet.vehicle_telemetry_events
fleet.vehicle_current_state
fleet.vehicle_health_alerts
fleet.vehicle_metrics_5m
```

## 2.15. Raw Telemetry Event Table

Potential:

```text
fleet.vehicle_telemetry_events
```

Fields:

```text
event_id
event_timestamp
vehicle_id
driver_id
latitude
longitude
speed_kmh
fuel_level_pct
engine_temperature_c
odometer_km
engine_status
vehicle_status
ingested_at
```

ClickHouse engine və `ORDER BY` design-ı access pattern-ə əsasən seç.

## 2.16. Vehicle Current State

Potential:

```text
fleet.vehicle_current_state
```

Latest state semantics üçün `ReplacingMergeTree` və ya uyğun pattern-i evaluate et.

## 2.17. Health Alerts

Potential:

```text
fleet.vehicle_health_alerts
```

Fields:

```text
event_id
vehicle_id
event_timestamp
alert_type
severity
observed_value
threshold
```

## 2.18. Windowed Metrics

Potential output:

```text
fleet.vehicle_metrics_5m
```

Example:

```text
window_start
window_end
active_vehicle_count
avg_speed_kmh
avg_fuel_level_pct
max_engine_temperature_c
overspeed_vehicle_count
low_fuel_vehicle_count
```

## 2.19. Flink SQL Sink

Mövcud Application 1-in ClickHouse sink pattern-ni inspect et.

Flink SQL application üçün sink table-ları mümkün qədər SQL DDL ilə idarə etməyin dəyərini analiz et.

## 2.20. Flink SQL Application Structure

Yeni Java app-i modular saxla.

Possible class:

```text
VehicleTelemetrySqlJob.java
```

Amma existing package convention-a uyğunlaşdır.

SQL management variantlarını müqayisə et:

```text
SQL embedded in Java constants
SQL files under resources/
mixed approach
```

Maintainability üçün ən uyğun variantı seç.

## 2.21. Recommended SQL Resources Structure

Potential:

```text
src/main/resources/sql/fleet/
├── source_vehicle_telemetry.sql
├── sink_vehicle_events.sql
├── sink_vehicle_state.sql
├── sink_vehicle_alerts.sql
└── vehicle_metrics_5m.sql
```

Amma SQL-ləri səbəbsiz çox file-a bölmə.

## 2.22. Application Isolation

Application 1 və Application 2 runtime-da ayrı Flink job olmalıdır.

```text
Flink Cluster
   |
   +--> DeliveryMonitoringJob
   |
   +--> VehicleTelemetrySqlJob
```

Bir job fail olduqda digər job-un business logic-i dayanmamalıdır.

## 2.23. Flink Job Naming

Professional naming standard istifadə et.

Məsələn:

```text
delivery-monitoring-stream
fleet-vehicle-telemetry-sql
```

## 2.24. Makefile

Yeni application üçün scalable target-lər təklif et.

Məsələn:

```bash
make submit-flink-job APP=delivery
make submit-flink-job APP=fleet
```

Hardcoded yüzlərlə Make target yaratma.

## 2.25. Producer Make Targets

Multi-application scale üçün generic parametrized Makefile pattern-ə üstünlük ver:

```bash
make produce APP=delivery
make produce APP=fleet
```

## 2.26. Configuration

Yeni app üçün bütün config-i `.env` daxilinə doldurma.

Application-specific config üçün metadata/config structure istifadə et.

Potential:

```yaml
application: fleet

kafka:
  topic: vehicle-telemetry-events

flink:
  job: vehicle-telemetry-sql

processing:
  overspeed_threshold_kmh: 100
  low_fuel_threshold_pct: 15
  engine_temperature_threshold_c: 105
```

Threshold-ların environment variable yoxsa app config olması barədə düzgün qərar ver.

## 2.27. Data Contract Structure

Scalable structure düşün:

```text
src/contracts/
├── delivery/
│   └── delivery_event_v1.schema.json
└── fleet/
    └── vehicle_telemetry_v1.schema.json
```

## 2.28. Testing

Minimum professional test coverage:

```text
Event contract validation
Telemetry event generation
Flink SQL startup / compile validation
SQL transformation tests where practical
ClickHouse output smoke test
End-to-end:
Producer -> Kafka -> Flink SQL -> ClickHouse
```

## 2.29. E2E Scenario

Vehicle:

```text
VEH-101
```

ardıcıl telemetry event-lər göndərir:

```text
10:00
speed = 62
fuel = 68%
temperature = 88C
NORMAL

10:03
speed = 108
fuel = 66%
temperature = 91C
OVERSPEED

10:06
speed = 75
fuel = 14%
temperature = 94C
LOW_FUEL

10:09
speed = 70
fuel = 13%
temperature = 108C
ENGINE_TEMPERATURE_HIGH
```

Nəticədə:

```text
raw telemetry history
latest vehicle state
alerts
window metrics
```

ClickHouse-da görünməlidir.

## 2.30. Superset Integration

Potential future BI datasets:

```text
Vehicle Health Overview
Vehicle Speed Trends
Fuel Level Monitoring
Engine Temperature Alerts
Fleet Health Alerts
Active Vehicle Metrics
```

Superset BI-as-Code modularlaşdırılıbsa fleet app üçün ayrıca scope istifadə et:

```text
configs/superset/fleet/
```

## 2.31. Architecture Goal

```text
                         Kafka

              +-----------+-----------+
              |                       |
              v                       v
       delivery-events      vehicle-telemetry-events
              |                       |
              v                       v
       Flink DataStream          Flink SQL
          Java Job              Java Job
              |                       |
              v                       v
       delivery schema            fleet schema
              |                       |
              +-----------+-----------+
                          |
                          v
                     ClickHouse
                          |
                          v
                       Superset
```

Bu architecture gələcəkdə üçüncü application əlavə olunanda da təkrar istifadə edilə bilməlidir.

## 2.32. Ən Vacib Scalability Məqsədi

Application 2-ni sadəcə hardcoded ikinci demo kimi yaratma.

Gələcək multi-application framework-in foundation-ını düşün.

Yeni application əsasən aşağıdakılarla əlavə edilə bilməlidir:

```text
Application metadata
Event contract
Business processing logic
Required target tables
Tests
```

Shared infrastructure reuse edilməlidir.

## 2.33. Generic ilə Application-Specific hissəni ayır

Shared:

```text
Kafka connectivity
configuration loading
logging
ClickHouse connectivity
Flink bootstrap
common validation
metrics
```

Fleet-specific:

```text
vehicle telemetry schema
vehicle health rules
fleet SQL transformations
fleet target tables
```

## 2.34. Implementation Sequence

Əvvəl repository-ni inspect et və təqdim et:

```text
1. Existing Flink project structure
2. Existing Kafka producer structure
3. Existing ClickHouse schema initialization
4. Existing Makefile Flink/producer targets
5. Existing event contract model
6. Second app üçün reuse edilə biləcək hissələr
7. Refactor edilməli hissələr
8. Proposed App 2 architecture
9. Proposed event contract
10. Proposed Kafka topic
11. Proposed ClickHouse tables
12. Proposed Java/Flink SQL project structure
13. Exact files that would need to be created/modified
```

Sonra yalnız **bir ilk implementation step** təklif et və approval gözlə.

## 2.35. Approval Rule

Heç bir meaningful dəyişiklik approval olmadan edilməməlidir.

Məsələn:

```text
Recommended first step:

Create fleet vehicle telemetry event contract.

Files:
src/contracts/fleet/vehicle_telemetry_v1.schema.json

Why:
Kafka producer və Flink SQL source schema bu contract-dan asılı olacaq.

Risk:
Low.

Do you approve?
```

## 2.36. Final Definition of Done

Application 2 yalnız aşağıdakı flow tam işlədikdə tamamlanmış hesab olunsun:

```text
1. Fleet telemetry contract mövcuddur.
2. Synthetic producer valid vehicle telemetry event yaradır.
3. Event ayrıca Kafka topic-ə publish olunur.
4. Java Flink SQL application topic-i consume edir.
5. Event-time və watermark konfiqurasiyası işləyir.
6. Flink SQL transformation-lar işləyir.
7. Raw telemetry ClickHouse-a yazılır.
8. Latest vehicle state hazırlanır.
9. Vehicle health alerts yaranır.
10. Window-based fleet metrics yaranır.
11. Application 1 eyni zamanda işləməyə davam edir.
12. Application 2 fail olduqda Application 1 business logic-i qırılmır.
13. E2E smoke test uğurla keçir.
14. Makefile-dan application-specific və ya parameterized run/submit əməliyyatı mümkündür.
15. Architecture gələcək üçüncü application əlavə etmək üçün copy/paste tələb etmir.
```

Əsas məqsəd budur:

> **DeliveryFlow daxilində mövcud Java Flink DataStream delivery application-a toxunmadan, ayrıca vehicle telemetry event domain-i üçün Java + Flink SQL/Table API əsasında ikinci müstəqil streaming application yarat və bunu gələcək multi-application architecture üçün nümunə/reference implementation kimi dizayn et.**
