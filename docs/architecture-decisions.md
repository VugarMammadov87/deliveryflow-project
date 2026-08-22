# Architecture Decisions

## ADR-000: Multi-Application Control Plane

DeliveryFlow is no longer documented as a single-application demo. The repository now has a small control-plane structure that separates application metadata, dataset metadata, and event contracts:

```text
configs/applications/
configs/datasets/
src/contracts/<domain>/
```

Decision:

- Keep the local platform simple, but make application ownership explicit.
- Keep delivery as the default application.
- Add fleet telemetry as an independent second application.
- Select non-default app runtime with `APP=fleet`.
- Keep Kafka topics domain-oriented, not one topic per table by default.
- Keep ClickHouse serving schemas domain-oriented: `delivery.*` and `fleet.*`.

The first implementation of this decision is `VehicleTelemetrySqlJob`, a Java Flink SQL/Table API job that reads `vehicle-telemetry-events` and writes to the `fleet` database. This is intentionally separate from `DeliveryStreamingJob`, which remains the Java DataStream implementation for delivery events.

This decision prepares the project for 300-500 table growth without requiring one Python script, one Spark job, one Flink job, one Airflow DAG, and one Superset file per table.

Bu sənəd DeliveryFlow layihəsində qəbul edilmiş əsas arxitektura qərarlarını izah edir. Qərarların məqsədi lokal lab üçün sadə, işlək və data engineering prinsiplərinə uyğun platforma qurmaqdır.

## Yüksək Səviyyəli Arxitektura

```mermaid
flowchart TB
    subgraph ingestion["Ingestion"]
        producer["Synthetic Producer"]
        kafka["Kafka"]
        postgres["PostgreSQL Source"]
    end

    subgraph processing["Processing"]
        flink["Flink Streaming"]
        spark["Spark Batch"]
        airflow["Airflow Orchestration"]
    end

    subgraph storage["Storage"]
        clickhouse["ClickHouse Serving"]
        iceberg["Iceberg Tables"]
        nessie["Nessie Catalog"]
        minio["MinIO Object Storage"]
    end

    subgraph bi["BI"]
        superset["Superset"]
    end

    producer --> kafka
    producer --> postgres
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

## Pinned Compatibility Set

Bu versiyalar lokal platformanın bir-biri ilə uyğun işləməsi üçün seçilib:

- Kafka: `apache/kafka:3.9.2`
- Flink: `flink:1.20.3-scala_2.12-java11`
- Flink Kafka connector: `org.apache.flink:flink-connector-kafka:3.4.0-1.20`
- Spark base: `apache/spark:3.5.6-java17`
- Spark local image: `deliveryflow-spark:3.5.6`
- Iceberg Spark runtime: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`
- Iceberg AWS bundle: `org.apache.iceberg:iceberg-aws-bundle:1.11.0`
- Nessie Spark extensions: `org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.3`
- Nessie server: `ghcr.io/projectnessie/nessie:0.107.5`
- Airflow: `apache/airflow:2.10.5-python3.10`
- Superset: `apache/superset:4.1.2`
- PostgreSQL: `postgres:16.4-bookworm`
- ClickHouse: `clickhouse/clickhouse-server:25.8`
- Object storage: `minio/minio:RELEASE.2025-04-22T22-12-26Z`

## ADR-001: Kafka KRaft Broker

Kafka bir KRaft broker kimi işləyir. Lokal lab üçün ZooKeeper əlavə edilməyib.

Səbəb:

- Daha az servis.
- Daha sadə local bootstrap.
- Müasir Kafka deployment modelinə daha yaxın setup.

Topic strategiyası:

```text
delivery-events
```

Bu topic delivery lifecycle event-lərini daşıyır. Event key `delivery_id`-dir.

## ADR-002: Flink Stream Processing

Flink real-time event processing üçün seçilib.

Səbəb:

- Kafka source ilə təbii inteqrasiya.
- Event-time və watermark support-u.
- Keyed stream processing.
- Checkpointing support.
- Real-time ClickHouse sink üçün uyğun model.

Flink bu layihədə aşağıdakı işi görür:

- Kafka event-lərini consume edir.
- JSON parse edir.
- `schema_version = 1` yoxlayır.
- Delivery state və vehicle state hazırlayır.
- ClickHouse-a yazır.

## ADR-003: ClickHouse Serving Layer

ClickHouse operational və BI serving qatıdır.

ClickHouse cədvəlləri:

- `delivery.delivery_events`: immutable event history.
- `delivery.delivery_current_state`: latest delivery state.
- `delivery.vehicle_current_state`: latest vehicle state.
- `delivery.daily_delivery_kpi`: batch KPI serving table.

ClickHouse view-ları:

- `v_delivery_status_overview`
- `v_delay_by_region`
- `v_vehicle_utilization`
- `v_warehouse_daily_kpi`
- `v_delivery_event_volume`

```mermaid
flowchart LR
    flink["Flink"] --> events["delivery_events"]
    flink --> deliveryState["delivery_current_state"]
    flink --> vehicleState["vehicle_current_state"]
    spark["Spark"] --> dailyKpi["daily_delivery_kpi"]

    events --> views["BI-ready views"]
    deliveryState --> views
    vehicleState --> views
    dailyKpi --> views
    views --> superset["Superset"]
```

## ADR-004: Spark Batch Processing

Spark daily KPI və lakehouse write üçün seçilib.

Səbəb:

- Batch aggregation üçün geniş ecosystem.
- Iceberg integration.
- JDBC ilə ClickHouse-dan oxuma imkanı.
- Airflow-dan `spark-submit` ilə idarə edilə bilməsi.

Spark job:

```text
src/etl/apps/daily_kpi_job.py
```

Spark output:

- `nessie.bronze.raw_delivery_events`
- `nessie.gold.daily_delivery_kpi`
- `delivery.daily_delivery_kpi`

## ADR-005: Iceberg + Nessie + MinIO Lakehouse

Lakehouse qatı üç hissədən ibarətdir:

- Iceberg table format.
- Nessie catalog.
- MinIO S3-compatible object storage.

```mermaid
flowchart LR
    spark["Spark"] --> iceberg["Iceberg table metadata"]
    iceberg --> nessie["Nessie catalog refs"]
    iceberg --> minio["MinIO warehouse files"]
    nessie --> pg["PostgreSQL nessie_metadata"]
```

Səbəb:

- Iceberg analytical table format verir.
- Nessie catalog və versioned metadata idarə edir.
- MinIO lokal object storage kimi S3-compatible davranır.

## ADR-006: PostgreSQL Database Separation

İki PostgreSQL servisi var:

- `postgres-airflow`
- `postgres-source`

`postgres-airflow` yalnız Airflow metadata üçündür.

`postgres-source` iki database saxlayır:

- `logistics_source`
- `nessie_metadata`

Bu ayrım lifecycle və ownership baxımından daha aydındır.

## ADR-007: Airflow Batch Orchestration

Airflow batch orchestration üçündür. Streaming path-a daxil deyil.

Airflow-un işi:

- DAG schedule etmək.
- Source readiness yoxlamaq.
- Spark job submit etmək.
- Task status və retry idarə etmək.

Bu layihədə Airflow `LocalExecutor` ilə işləyir.

## ADR-008: Superset BI Layer

Superset BI və dashboard qatıdır.

Superset ClickHouse-a qoşulur və prepared view-lardan chart qurur.

Səbəb:

- Compose daxilində lokal işləyir.
- ClickHouse ilə uyğundur.
- Dashboard-lar browser üzərindən açılır.
- Platforma tam lokal qalır.

Superset raw Kafka, raw MinIO və ya Iceberg metadata ilə işləməməlidir. Onun approved data source-u ClickHouse serving qatıdır.

BI obyektləri manual UI konfiqurasiyası kimi saxlanmır. `configs/superset/deliveryflow_bi.yaml` Superset database, dataset, chart və dashboard definition-ları üçün repository source of truth-dur.

`make import-superset-assets` import workflow-u aşağıdakı qərarları tətbiq edir:

- `superset-importer` image əvvəl rebuild olunur ki, YAML və Python importer dəyişiklikləri köhnə image-də qalmasın.
- YAML-dəki metric adları Superset saved metric kimi yox, chart-level adhoc metric kimi yazılır. Bu `Metric 'event_count' does not exist` və `Metric 'delay_rate' does not exist` xətalarının qarşısını alır.
- Count/sum column-ları üçün `SUM(...)`, rate və average column-lar üçün `AVG(...)` seçilir.
- Timeseries chart-lar üçün temporal metadata açıq saxlanılır. `v_delivery_event_volume.event_hour` dataset-də `main_dttm_col` və temporal column-dur, chart-da isə `x_axis` və `granularity_sqla` dəyəridir.
- `business_date` warehouse KPI dataset-də temporal column kimi qeyd olunur ki, gələcək time-based chart-lar eyni qayda ilə işləsin.

Bu qərarın məqsədi Superset dashboard-un təmiz environment-də də reproducible import olunmasıdır: dataset-lər, chart-lar və dashboard yaransın, chart render zamanı metric və datetime metadata xətası verməsin.

## ADR-009: One Generator, Two Data Types

Generator tək app olaraq saxlanılıb:

```text
src/producers/synthetic_logistics_producer.py
```

Amma iki data tipi yaradır:

- Batch source rows.
- Stream events.

Bu qərarın səbəbi:

- Eyni synthetic business domain qorunur.
- Batch və stream data bir-biri ilə məntiqi uyğun qalır.
- Lokal lab-da əlavə app complexity yaranmır.

## ADR-010: `make clean` və `make purge` Ayrımı

`make clean` non-destructive qalır:

```text
docker compose down --remove-orphans
```

`make purge` destructive reset üçündür:

```text
docker compose down --volumes --remove-orphans --rmi local
```

Səbəb:

- Normal cleanup data volume-ları silməməlidir.
- Schema dəyişiklikləri zamanı tam sıfırlama üçün ayrıca açıq target lazımdır.

## Runtime Service Map

```mermaid
flowchart TB
    subgraph compose["Docker Compose deliveryflow"]
        kafka["Kafka"]
        kafkaUi["Kafka UI"]
        producer["Producer"]
        flinkJm["Flink JobManager"]
        flinkTm["Flink TaskManager"]
        sparkMaster["Spark Master"]
        sparkWorker["Spark Worker"]
        airflowWeb["Airflow Webserver"]
        airflowScheduler["Airflow Scheduler"]
        clickhouse["ClickHouse"]
        superset["Superset"]
        minio["MinIO"]
        nessie["Nessie"]
        pgAirflow["Postgres Airflow"]
        pgSource["Postgres Source"]
    end

    kafka --> kafkaUi
    producer --> kafka
    producer --> pgSource
    kafka --> flinkJm
    flinkJm --> flinkTm
    flinkJm --> clickhouse
    airflowScheduler --> sparkMaster
    sparkMaster --> sparkWorker
    sparkMaster --> clickhouse
    sparkMaster --> minio
    sparkMaster --> nessie
    nessie --> pgSource
    airflowWeb --> pgAirflow
    airflowScheduler --> pgAirflow
    superset --> clickhouse
```

## Operasional Qaydalar

- Runtime config dəyişikliklərindən sonra `docker compose config --quiet` yoxlanmalıdır.
- Schema dəyişikliklərindən sonra köhnə volume varsa `make purge` tələb oluna bilər.
- Dashboard üçün ClickHouse view-ları əsas götürülməlidir.
- Batch job üçün source readiness lazımdır; ClickHouse `delivery_events` boşdursa KPI job fail edə bilər.
- Streaming üçün əvvəl Flink job submit edilməli, sonra producer işə salınmalıdır.
