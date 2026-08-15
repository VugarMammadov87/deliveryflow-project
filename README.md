# DeliveryFlow Local Data Platform

DeliveryFlow lokal Docker Compose üzərində qurulmuş data engineering layihəsidir. Layihə logistika domenində həm real-time delivery monitoring, həm də gündəlik batch analytics proseslərini göstərir.

Bu README layihəyə ilk dəfə baxan biri üçün yazılıb: haradan başlamaq lazımdır, hansı servis nə edir, `make` komandaları hansı ardıcıllıqla işlədilir və pipeline-lar necə yoxlanılır.

## Qısa Xülasə

Layihədə iki əsas data axını var:

- **Stream pipeline**: `Producer -> Kafka -> Flink -> ClickHouse -> Superset`
- **Batch pipeline**: `ClickHouse/PostgreSQL -> Spark -> Iceberg/Nessie/MinIO -> ClickHouse -> Superset`

Əsas servis rolları:

- **Kafka** event transport qatıdır.
- **Flink** real-time stream processing edir.
- **ClickHouse** operational və BI serving database-dir.
- **Spark** batch KPI hesablayır.
- **Iceberg** analytical table format verir.
- **Nessie** Iceberg catalog-dur.
- **MinIO** lokal S3-compatible object storage-dur.
- **Airflow** batch job orchestration edir.
- **PostgreSQL** Airflow, Nessie və source metadata saxlayır.
- **Superset** dashboard və reporting qatıdır.

## Haradan Başlamaq Lazımdır?

Əgər layihəni ilk dəfə açırsansa, bu ardıcıllıqla get:

1. Repo root folder-də olduğunu yoxla.
2. `.env.example` faylından `.env` yarat.
3. Docker Compose config-i validate et.
4. Platformanı qaldır.
5. Servislərin health vəziyyətinə bax.
6. Flink stream job-u submit et.
7. Synthetic data yarat.
8. ClickHouse-da stream nəticələrini yoxla.
9. Spark batch KPI job-u işlət.
10. Superset-də report view-ları dashboard üçün istifadə et.

Əsas command axını:

```powershell
Copy-Item .env.example .env
make config
make up
make health
make submit-flink-job
make produce
make spark-daily-kpi
make console
```

## Arxitektura Diaqramı

```mermaid
flowchart LR
    producer["Synthetic Logistics Producer<br/>Python"]
    postgres["PostgreSQL<br/>Source Tables"]
    kafka["Kafka<br/>delivery-events topic"]
    flink["Flink DataStream Job<br/>Java"]
    clickhouse["ClickHouse<br/>Serving Layer"]
    spark["Spark Daily KPI Job<br/>PySpark"]
    iceberg["Apache Iceberg Tables<br/>bronze / gold"]
    nessie["Nessie<br/>Iceberg Catalog"]
    minio["MinIO<br/>S3-compatible Storage"]
    airflow["Airflow<br/>Batch Orchestration"]
    superset["Superset<br/>BI Dashboards"]

    producer -->|batch seed| postgres
    producer -->|stream events| kafka
    kafka --> flink
    flink --> clickhouse
    airflow --> spark
    clickhouse --> spark
    spark --> iceberg
    iceberg --> nessie
    iceberg --> minio
    spark --> clickhouse
    clickhouse --> superset
```

## Runtime Service Map

```mermaid
flowchart TB
    subgraph compose["Docker Compose: deliveryflow"]
        subgraph streaming["Streaming Layer"]
            kafka["Kafka Broker"]
            kafkaUi["Kafka UI"]
            producer["Producer Container"]
            flinkJm["Flink JobManager"]
            flinkTm["Flink TaskManager"]
            flinkSubmit["Flink Job Submitter"]
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
|   |-- spark/
|   `-- superset/
|-- src/
|   |-- contracts/
|   |   `-- delivery_event_v1.schema.json
|   |-- etl/
|   |   `-- apps/
|   |       |-- daily_kpi_job.py
|   |       `-- iceberg_smoke_test.py
|   `-- producers/
|       `-- synthetic_logistics_producer.py
|-- tests/
|   `-- test_event_contract.py
|-- docker-compose.yml
|-- Makefile
|-- README.md
|-- requirement.md
`-- plan.md
```

## Prerequisites

Lokal işlətmək üçün lazımdır:

- Docker Desktop
- Docker Compose v2
- `make`
- Git
- Kifayət qədər RAM və disk

Port conflict olarsa `.env` içində portları dəyişmək olar. Ən çox konflikt yaradan portlar:

- `AIRFLOW_WEB_PORT=8080`
- `FLINK_UI_PORT=8081`
- `SPARK_MASTER_UI_PORT=8082`
- `KAFKA_UI_PORT=8083`
- `SUPERSET_PORT=8088`
- `POSTGRES_AIRFLOW_PORT=15432`
- `POSTGRES_SOURCE_PORT=15433`

## Addım 1: Environment Faylını Hazırla

İlk dəfə başlamazdan əvvəl:

```powershell
Copy-Item .env.example .env
```

`.env` faylı Docker Compose üçün bütün local config-ləri saxlayır:

- image adları
- portlar
- database adları
- local user/password dəyərləri
- Kafka topic adı
- producer parametrləri
- Superset parametrləri

`.env` git-ə commit edilməməlidir.

## Addım 2: Compose Config-i Validate Et

Platformanı qaldırmazdan əvvəl:

```powershell
make config
```

Bu command əslində bunu işlədir:

```text
docker compose config
```

Nə üçün lazımdır:

- `.env` dəyərləri düzgün oxunurmu?
- `docker-compose.yml` sintaksisi doğrudurmu?
- volume, port, service və environment mapping-lərində səhv varmı?

Əgər burada error varsa, `make up` etməzdən əvvəl düzəltmək lazımdır.

## Addım 3: Platformanı Qaldır

Əsas start command:

```powershell
make up
```

Bu command aşağıdakı servisləri build və start edir:

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

İlk start zamanı image build və pull səbəbindən proses uzun çəkə bilər.

## Addım 4: Servis Statusuna Bax

Container status:

```powershell
make ps
```

və ya:

```powershell
make status
```

Bu command `docker compose ps` işlədir.

Health check:

```powershell
make health
```

Bu command producer container içindən `scripts/health_check.py` scriptini işlədir və əsas servisləri yoxlayır:

- Kafka
- PostgreSQL
- MinIO
- Nessie
- Spark
- ClickHouse
- Superset
- Flink
- Airflow

## Addım 5: URL-ləri Götür

Bütün endpoint-ləri görmək üçün:

```powershell
make console
```

Yalnız browser URL-ləri üçün:

```powershell
make urls
```

Əsas URL-lər:

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

## Addım 6: Stream Pipeline-ı İşlət

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

    P->>K: delivery event publish edilir
    K->>F: Flink event consume edir
    F->>F: JSON parse və schema validation
    F->>C: delivery_events yazılır
    F->>C: delivery_current_state yazılır
    F->>C: vehicle_current_state yazılır
```

Əvvəl Flink job-u submit et:

```powershell
make submit-flink-job
```

Sonra data yarat:

```powershell
make produce
```

`make produce` default olaraq həm batch source data yaradır, həm də Kafka stream event-ləri publish edir.

Yalnız stream event yaratmaq istəyirsənsə:

```powershell
make produce-stream
```

Nəticəni ClickHouse-da yoxlamaq:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_events"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_current_state"
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.vehicle_current_state"
```

## Addım 7: Batch Source Data Yarat

PostgreSQL source cədvəllərini ayrıca seed etmək üçün:

```powershell
make seed-batch-source
```

Bu command `PRODUCER_MODE=batch` ilə generator app-i işlədir.

PostgreSQL source cədvəllərinə baxmaq:

```powershell
docker compose exec postgres-source psql -U postgres -d logistics_source -c "\dt"
```

Order sample:

```powershell
docker compose exec postgres-source psql -U postgres -d logistics_source -c "SELECT order_id, customer_region, service_level, priority, package_count, order_value FROM customer_orders LIMIT 10;"
```

Bu cədvəllər haqqında geniş izah:

```text
docs/postgres-source-tables.md
```

## Addım 8: Iceberg Connectivity Test Et

Spark, Nessie, Iceberg və MinIO birlikdə düzgün işləyirmi yoxlamaq üçün:

```powershell
make spark-iceberg-test
```

Uğurlu nəticədə gözlənən marker:

```text
ICEBERG_SMOKE_TEST_OK
```

Bu test nəyi yoxlayır:

- Spark session açılır.
- Nessie catalog-a qoşulur.
- Iceberg namespace/table əməliyyatları işləyir.
- MinIO warehouse path istifadə olunur.

## Addım 9: Batch KPI Job İşlət

Daily KPI job:

```powershell
make spark-daily-kpi
```

Bu command `spark-master` container içində `spark-submit` işlədir. Bu ona görə belə qurulub ki, Spark job Spark image-in öz classpath-i, `spark-defaults.conf` faylı və `/opt/deliveryflow/src/etl/apps/` içindəki app faylları ilə işləsin.

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

    S->>C: delivery.delivery_events oxuyur
    S->>S: KPI hesablayır
    S->>I: bronze.raw_delivery_events yazır
    S->>I: gold.daily_delivery_kpi yazır
    I->>N: table metadata commit
    I->>M: data və metadata files
    S->>C: delivery.daily_delivery_kpi publish edir
    BI->>C: dashboard data oxuyur
```

Uğurlu nəticədə gözlənən marker:

```text
DAILY_KPI_JOB_OK
```

KPI nəticəsini yoxlamaq:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM delivery.daily_delivery_kpi LIMIT 10"
```

## Addım 10: Superset Dashboard Üçün Data Mənbələri

Superset URL:

```text
http://localhost:8088
```

ClickHouse connection URI:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

Superset obyektlərini BI-as-code kimi import etmək:

```powershell
make import-superset-assets
```

Bu command `configs/superset/deliveryflow_bi.yaml` faylından database, dataset, chart və dashboard obyektlərini Superset REST API vasitəsilə yaradır və ya yeniləyir.

Dashboard üçün hazır view-lar:

- `delivery.v_delivery_status_overview`
- `delivery.v_delay_by_region`
- `delivery.v_vehicle_utilization`
- `delivery.v_warehouse_daily_kpi`
- `delivery.v_delivery_event_volume`

View-ları yoxlamaq:

```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

Report və chart izahları:

```text
docs/superset-serving.md
```

## Addım 11: End-to-End Smoke Test

Stream path üçün smoke test:

```powershell
make test-e2e
```

Bu command:

- producer ilə synthetic event yaradır
- ClickHouse-da `delivery_events` row gözləyir
- `delivery_current_state` row gözləyir
- stream path-in işlədiyini təsdiqləyir

Gözlənən marker:

```text
STREAMING_E2E_OK
```

## Make Komandalarının Praktik İstifadəsi

### `make help`

Layihədə mövcud make command-larını göstərir.

```powershell
make help
```

### `make config`

Docker Compose config-i validate edir. `.env` və `docker-compose.yml` dəyişəndən sonra işlət.

```powershell
make config
```

### `make pull`

Pinned upstream image-ləri pull edir. İlk setup və ya image cache köhnə olanda faydalıdır.

```powershell
make pull
```

### `make build`

Local image-ləri build edir: Spark, Airflow, Flink, Producer və Superset.

```powershell
make build
```

### `make up`

Full local platformanı başladır.

```powershell
make up
```

Bu command core servislərlə birlikdə `producer-continuous` servisini də başladır. Həmin servis Kafka-ya hər 10 dəqiqədən bir yeni stream event göndərir.

### `make ps` və `make status`

Container-lərin statusunu göstərir.

```powershell
make ps
make status
```

### `make health`

Əsas servislərin readiness vəziyyətini yoxlayır.

```powershell
make health
```

### `make console` və `make urls`

Servis endpoint-lərini göstərir.

```powershell
make console
make urls
```

### `make submit-flink-job`

Flink streaming job-u submit edir. Kafka event-ləri generate etməzdən əvvəl işlətmək lazımdır.

```powershell
make submit-flink-job
```

### `make produce`

Default generator mode ilə həm PostgreSQL batch source row-ları yaradır, həm də Kafka stream event-ləri publish edir.

```powershell
make produce
```

### `make produce-stream`

Yalnız Kafka stream event-ləri yaradır.

```powershell
make produce-stream
```

### `make produce-continuous`

Background `producer-continuous` servisini başladır. Bu servis `PRODUCER_MODE=stream` və `PRODUCER_CONTINUOUS=true` ilə işləyir, default olaraq hər 10 dəqiqədən bir Kafka-ya delivery event göndərir.

```powershell
make produce-continuous
```

Interval `.env` içində dəyişdirilir:

```text
PRODUCER_CONTINUOUS_INTERVAL_SECONDS=600
```

Log-lara baxmaq:

```powershell
make logs-producer-continuous
```

Dayandırmaq:

```powershell
make stop-continuous-producer
```

### `make seed-batch-source`

Yalnız PostgreSQL source cədvəllərini seed edir.

```powershell
make seed-batch-source
```

### `make spark-iceberg-test`

Spark, Iceberg, Nessie və MinIO bağlantısını smoke test edir.

```powershell
make spark-iceberg-test
```

### `make spark-daily-kpi`

Daily KPI batch job-u işlədir. ClickHouse-da `delivery_events` data-sı olduqdan sonra işlət.

```powershell
make spark-daily-kpi
```

### `make import-superset-assets`

Superset obyektlərini repo-dakı YAML faylından yaradır və ya yeniləyir.

```powershell
make import-superset-assets
```

YAML source of truth:

```text
configs/superset/deliveryflow_bi.yaml
```

Import nəticəsində `DeliveryFlow ClickHouse` database connection, 5 dataset, 5 chart və `DeliveryFlow Operations Dashboard` dashboard-u Superset-də hazır olur.

### `make airflow-dag-list`

Airflow daxilində DAG siyahısını göstərir.

```powershell
make airflow-dag-list
```

### `make test-e2e`

Stream path üçün end-to-end smoke test edir.

```powershell
make test-e2e
```

### Log Command-ları

Servis log-larını izləmək üçün:

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

Container-ləri stop edir, amma named volume-ları saxlayır.

```powershell
make down
```

### `make clean`

Container-ləri və orphan container-ləri silir, amma named volume-ları saxlayır.

```powershell
make clean
```

### `make purge`

Destructive cleanup edir:

- container-ləri silir
- named volume-ları silir
- local project image-ləri silir
- orphan container-ləri silir

```powershell
make purge
```

Diqqət: `make purge` lokal data-nı silir.

## Tövsiyə Edilən Tam Demo Ardıcıllığı

Sıfırdan demo üçün:

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
make console
```

Sonra browser-də aç:

- Kafka UI: `http://localhost:8083`
- Flink UI: `http://localhost:8081`
- Spark UI: `http://localhost:8082`
- Airflow UI: `http://localhost:8080`
- Superset UI: `http://localhost:8088`

## Debug Workflow

Əgər data ClickHouse-a gəlmirsə:

1. Kafka log-larına bax:

```powershell
make logs-kafka
```

2. Flink job log-larına bax:

```powershell
make logs-flink
```

3. Stream event yarat:

```powershell
make produce-stream
```

4. ClickHouse row count yoxla:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM delivery.delivery_events"
```

Əgər Superset view-ları görünmürsə:

```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

Əgər yeni schema görünmürsə, köhnə volume qalır. Lokal data-nı silmək qəbul edilirsə:

```powershell
make purge
make up
```

## Əlavə Sənədlər

Daha dərin oxumaq üçün:

- `docs/application-workflow.md`: app, generator, stream və batch workflow.
- `docs/postgres-source-tables.md`: PostgreSQL source cədvəlləri və inspect command-ları.
- `docs/superset-serving.md`: Superset dashboard strategy və 5 report.
- `docs/architecture-decisions.md`: servis seçimləri və ADR-lər.
- `requirement.md`: ümumi requirement.
- `plan.md`: layihə planı.

## Vacib Qeydlər

- Streaming üçün Flink job əvvəl submit edilməlidir, sonra event generate etmək daha düzgündür.
- Batch KPI üçün ClickHouse-da `delivery_events` data-sı olmalıdır.
- Superset üçün approved data source ClickHouse-dur.
- Kafka analytical database deyil, event transport qatıdır.
- Iceberg/Nessie/MinIO analytical lakehouse qatıdır.
- `make clean` data volume-ları silmir.
- `make purge` data volume-ları silir.
