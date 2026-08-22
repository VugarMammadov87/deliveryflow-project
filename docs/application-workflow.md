# DeliveryFlow Application Workflow

Bu sənəd DeliveryFlow layihəsində tətbiqlərin, servislərin, batch və stream proseslərinin necə işlədiyini addım-addım izah edir. Məqsəd yalnız "hansı servis var" sualına cavab vermək deyil; məqsəd data-nın hansı mənbədən çıxdığını, hansı servisdən keçdiyini, harada saxlandığını və Superset-də necə hesabat kimi göründüyünü aydın göstərməkdir.

## Ümumi Məntiq

DeliveryFlow lokal Docker Compose üzərində qurulmuş logistika data platformasıdır. Platforma eyni biznes domenini iki fərqli data axını ilə göstərir:

- **Stream processing**: real-time delivery event-ləri Kafka-dan Flink-ə, oradan ClickHouse-a gedir.
- **Batch processing**: source və serving data Spark tərəfindən oxunur, Iceberg/Nessie/MinIO qatına yazılır və KPI nəticələri yenidən ClickHouse-a publish edilir.

```mermaid
flowchart LR
    generator["Synthetic Logistics Generator"]
    postgres["PostgreSQL<br/>logistics_source"]
    kafka["Kafka<br/>delivery-events"]
    flink["Flink<br/>stream processing"]
    clickhouse["ClickHouse<br/>serving layer"]
    airflow["Airflow<br/>batch orchestration"]
    spark["Spark<br/>batch KPI job"]
    iceberg["Iceberg Tables<br/>bronze / gold"]
    nessie["Nessie<br/>catalog"]
    minio["MinIO<br/>object storage"]
    superset["Superset<br/>dashboards"]

    generator -->|batch seed| postgres
    generator -->|stream events| kafka
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

## Bir Generator App

Generator faylı:

```text
src/producers/synthetic_logistics_producer.py
```

Bu app iki data tipi yaradır:

- PostgreSQL üçün batch/source data.
- Kafka üçün stream delivery event-ləri.

Mode seçimi `PRODUCER_MODE` ilə edilir:

- `stream`: yalnız Kafka event-ləri publish edir.
- `batch`: yalnız PostgreSQL source cədvəllərini seed edir.
- `both`: əvvəl PostgreSQL seed edir, sonra Kafka event-ləri publish edir.

Default local mode:

```text
PRODUCER_MODE=both
```

Generatorun yaratdığı event-lər logistika prosesinin müxtəlif mərhələlərini simulyasiya edir:

- `ORDER_LOADED`
- `VEHICLE_DEPARTED`
- `IN_TRANSIT`
- `DELIVERY_DELAYED`
- `DELIVERED`

Event payload-u artıq yalnız status və koordinatlardan ibarət deyil. Hesabat üçün daha zəngin sahələr var:

- `service_level`
- `priority`
- `customer_id`
- `destination_city`
- `package_count`
- `order_value`
- `payment_method`
- `planned_distance_km`
- `traffic_condition`
- `weather_condition`

### Davamli Stream Rejimi

Stream generator bir defe isleyib dayanmaqla yanashi davamli servis kimi de isleyir. `producer-continuous` servisi `PRODUCER_MODE=stream` ve `PRODUCER_CONTINUOUS=true` ile baslayir.

Default interval:

```text
PRODUCER_CONTINUOUS_INTERVAL_SECONDS=600
```

Bu o demekdir ki, platforma qalxandan sonra her 10 deqiqeden bir Kafka `delivery-events` topic-ine yeni logistics event publish edilir. Bu rejim Superset-de event volume, latest delivery state ve vehicle utilization chart-larinin zamanla yenilenmesini yoxlamaq ucundur.

Davamli producer-i elle baslatmaq:

```powershell
make produce-continuous
```

Log-lara baxmaq:

```powershell
make logs-producer-continuous
```

Davamli producer-i dayandirmaq:

```powershell
make stop-continuous-producer
```

## Stream Pipeline

Stream pipeline real-time monitoring üçündür.

```text
Generator -> Kafka -> Flink -> ClickHouse -> Superset
```

```mermaid
sequenceDiagram
    participant G as Generator
    participant K as Kafka
    participant F as Flink
    participant CH as ClickHouse
    participant S as Superset

    G->>K: JSON delivery event publish edilir
    K->>F: Flink delivery-events topic-indən consume edir
    F->>F: JSON parse və schema_version yoxlanılır
    F->>F: Stream delivery_id ilə keyBy edilir
    F->>CH: delivery_events tarixi yazılır
    F->>CH: delivery_current_state yenilənir
    F->>CH: vehicle_current_state yenilənir
    S->>CH: Dashboard sorğuları ClickHouse-dan oxuyur
```

### Kafka Rolu

Kafka domain event transport qatıdır. Bu layihədə əsas topic:

```text
delivery-events
```

Event-lər `delivery_id` ilə keyed publish edilir. Bunun məqsədi eyni delivery üçün event ardıcıllığını lokal lab səviyyəsində daha stabil saxlamaqdır.

### Flink Rolu

Flink real-time processing üçündür. Əsas job:

```text
services/flink/src/main/java/local/deliveryflow/DeliveryStreamingJob.java
```

Flink job-un məsuliyyətləri:

- Kafka-dan event oxumaq.
- JSON payload-u `DeliveryEventParser` ilə parse etmək.
- `schema_version = 1` olan event-ləri qəbul etmək.
- Event-ləri `delivery_id` ilə key etmək.
- ClickHouse-a history və latest-state yazmaq.

Flink sink:

```text
services/flink/src/main/java/local/deliveryflow/ClickHouseDeliverySink.java
```

Bu sink üç cədvələ yazır:

- `delivery.delivery_events`
- `delivery.delivery_current_state`
- `delivery.vehicle_current_state`

## Batch Pipeline

Batch pipeline gündəlik analitika və lakehouse yazılışı üçündür.

```text
ClickHouse -> Spark -> Iceberg/Nessie/MinIO -> ClickHouse -> Superset
```

```mermaid
sequenceDiagram
    participant A as Airflow
    participant CH as ClickHouse
    participant SP as Spark
    participant I as Iceberg
    participant N as Nessie
    participant M as MinIO
    participant S as Superset

    A->>CH: delivery_events boş deyilmi?
    A->>SP: daily_kpi_job.py spark-submit
    SP->>CH: delivery.delivery_events oxunur
    SP->>SP: enrichment və KPI hesablanır
    SP->>I: bronze.raw_delivery_events yazılır
    SP->>I: gold.daily_delivery_kpi yazılır
    I->>N: metadata commit edilir
    I->>M: data və metadata faylları saxlanır
    SP->>CH: daily_delivery_kpi publish edilir
    S->>CH: dashboard üçün KPI oxunur
```

### Spark Rolu

Spark batch computation engine-dir. Əsas job:

```text
src/etl/apps/daily_kpi_job.py
```

Spark job ClickHouse-dan delivery event history-ni oxuyur, sonra bu metrikləri hesablayır:

- completed deliveries
- delayed deliveries
- on-time deliveries
- delay rate
- on-time rate
- average delay minutes
- average vehicle utilization
- total order value
- total package count
- average distance km

Spark nəticələri iki yerə yazır:

- Iceberg analytical tables: uzunmüddətli lakehouse saxlanması.
- ClickHouse KPI table: Superset üçün sürətli serving.

### Airflow Rolu

Airflow streaming yolunda iştirak etmir. Onun işi batch workflow-u schedule və monitor etməkdir.

DAG faylı:

```text
dags/delivery_daily_kpi.py
```

DAG task-ları:

1. `check_source_readiness`: ClickHouse-da `delivery_events` data-sının olduğunu yoxlayır.
2. `run_spark_daily_batch`: Spark daily KPI job-u submit edir.

## Storage və Serving Qatları

```mermaid
flowchart TB
    subgraph source["Source / Metadata"]
        pgsrc["PostgreSQL<br/>logistics_source"]
        pgnessie["PostgreSQL<br/>nessie_metadata"]
    end

    subgraph streamServing["Operational Serving"]
        chEvents["delivery_events"]
        chDelivery["delivery_current_state"]
        chVehicle["vehicle_current_state"]
    end

    subgraph lakehouse["Lakehouse"]
        bronze["bronze.raw_delivery_events"]
        gold["gold.daily_delivery_kpi"]
        nessie["Nessie catalog"]
        minio["MinIO warehouse"]
    end

    subgraph bi["BI Serving"]
        kpi["daily_delivery_kpi"]
        views["Superset report views"]
        superset["Superset dashboards"]
    end

    pgsrc --> spark["Spark batch"]
    chEvents --> spark
    spark --> bronze
    spark --> gold
    bronze --> nessie
    gold --> nessie
    bronze --> minio
    gold --> minio
    spark --> kpi
    chEvents --> views
    chDelivery --> views
    chVehicle --> views
    kpi --> views
    views --> superset
```

## Əsas İş Axınları

### Platformanı Başlatmaq

```powershell
make up
```

Bu command Docker Compose servislərini build və start edir.

### Stream Job Submit Etmək

```powershell
make submit-flink-job
```

Bu command Flink job-u JobManager-ə submit edir.

### Data Yaratmaq

Batch və stream birlikdə:

```powershell
make produce
```

Yalnız stream:

```powershell
make produce-stream
```

Yalnız PostgreSQL batch source:

```powershell
make seed-batch-source
```

### Batch KPI Job İşlətmək

```powershell
make spark-daily-kpi
```

Bu command Spark job-u işlədir və KPI nəticələrini həm Iceberg, həm ClickHouse tərəfinə yazır.

## Harada Nəyə Baxmaq Lazımdır

Servis UI-ları:

- Kafka UI: `http://localhost:8083`
- Flink UI: `http://localhost:8081`
- Spark UI: `http://localhost:8082`
- Airflow UI: `http://localhost:8080`
- Superset UI: `http://localhost:8088`
- Superset BI-as-code import:

```powershell
make import-superset-assets
```

Bu import `configs/superset/deliveryflow_bi.yaml` faylından database connection, 5 dataset, 5 chart və `DeliveryFlow Operations Dashboard` obyektlərini yaradır və ya yeniləyir. Importer chart metric-lərini Superset adhoc metric formatına çevirir və timeseries chart üçün `event_hour` datetime axis metadata-sını yazır.
- MinIO Console: `http://localhost:9001`

Database inspection:

```powershell
docker compose exec postgres-source psql -U postgres -d logistics_source -c "\dt"
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

## Ən Vacib Prinsiplər

- Kafka event transport üçündür, analytical source of truth deyil.
- Flink real-time state və operational history yazır.
- Spark batch aggregation və lakehouse yazılışı edir.
- Iceberg/Nessie/MinIO uzunmüddətli analytical storage qatıdır.
- ClickHouse serving qatıdır.
- Superset dashboard qatıdır.
- Airflow batch orchestration qatıdır.
