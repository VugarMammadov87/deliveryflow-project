# DeliveryFlow Local Data Platform

DeliveryFlow is a local Docker Compose data platform for real-time delivery monitoring and daily transportation analytics.

## Quick Start

```powershell
docker desktop start
make config
make up
make submit-flink-job
make produce
make health
make spark-iceberg-test
make spark-daily-kpi
```

## Main Commands

- `make up` starts the platform and preserves named volumes.
- `make down` stops the platform and preserves named volumes.
- `make health` checks major service readiness.
- `make status` or `make ps` shows containers.
- `make console` prints service endpoints.
- `make logs-*` tails service logs.

## Architecture

- Kafka transports versioned JSON logistics events.
- Flink processes streaming events and writes operational state to ClickHouse.
- MinIO provides S3-compatible Data Lake storage.
- Iceberg stores Parquet analytical tables.
- Nessie manages the Iceberg catalog using a PostgreSQL-backed metadata database.
- Spark performs batch computation using PySpark application files baked into the local Spark image.
- Airflow LocalExecutor orchestrates Spark jobs using the dedicated PostgreSQL metadata service.
- A separate PostgreSQL service creates the ETL source database, source tables, and the Nessie metadata database.
- Power BI connects to prepared ClickHouse serving tables.

## Local Credentials

The checked-in `.env.example` contains placeholders and local-only examples. The local `.env` file is ignored by git and contains development-only credentials.
