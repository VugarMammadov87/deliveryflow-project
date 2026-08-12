# Architecture Decisions

This document records the local bootstrap decisions derived from `requirement.md` and `plan.md`.

## Pinned Compatibility Set

- Kafka: `apache/kafka:3.9.2`
- Flink: `flink:1.20.3-scala_2.12-java11`
- Flink Kafka connector: `org.apache.flink:flink-connector-kafka:3.4.0-1.20`
- Spark base: `apache/spark:3.5.6-java17`
- Spark local image: `deliveryflow-spark:3.5.6`
- Iceberg Spark runtime: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`
- Iceberg AWS bundle: `org.apache.iceberg:iceberg-aws-bundle:1.11.0`
- Nessie Spark extensions: `org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.105.3` (the latest resolvable Maven Central artifact for the Spark 3.5 / Scala 2.12 coordinate)
- Nessie server: `ghcr.io/projectnessie/nessie:0.107.5`
- Airflow: `apache/airflow:2.10.5-python3.11`
- PostgreSQL: `postgres:16.4-bookworm`
- ClickHouse: `clickhouse/clickhouse-server:25.8`
- Object storage: `minio/minio:RELEASE.2025-04-22T22-12-26Z`

## Local ADRs Closed for Bootstrap

Kafka runs as one KRaft broker with separate internal and host listeners. Topic strategy starts with one domain topic, `delivery-events`, keyed by `delivery_id` to preserve per-delivery ordering in the local lab.

Flink uses a Java DataStream job. This is the most direct way to use event time, watermarks, keyed state, Kafka source semantics, checkpointing, and explicit ClickHouse writes without adding a separate SQL gateway.

Object storage uses MinIO because it is simple, S3-compatible, and works with Spark/Iceberg through the S3A/S3 FileIO path.

PostgreSQL uses two local instances with separate lifecycle and volume boundaries: `postgres-airflow` stores only Airflow metadata in `airflow_metadata`, while `postgres-source` owns `logistics_source` for ETL source tables and `nessie_metadata` for Nessie catalog metadata.

ClickHouse separates immutable event history from current state using `MergeTree` for history and `ReplacingMergeTree` for latest-state tables.

Iceberg uses a Nessie catalog named `nessie`, warehouse `s3://delivery-lakehouse/warehouse`, namespace tiers `bronze`, `silver`, and `gold`, and day-level partitions for local daily logistics data. Nessie stores its version metadata in PostgreSQL through the JDBC2 version store.

Airflow uses `LocalExecutor` backed by `postgres-airflow` and submits Spark jobs using application files baked into the local Spark image under `/opt/deliveryflow/src/etl/apps`.

Power BI is not containerized. It connects to ClickHouse host HTTP/native endpoints and uses the prepared operational and analytical tables.
