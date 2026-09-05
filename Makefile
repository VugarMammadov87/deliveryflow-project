# DeliveryFlow Local Platform Developer Makefile
# Provides standardized commands for local Docker Compose lifecycle,
# synthetic event generation, Flink streaming, Spark batch ETL, and Superset BI assets.

SHELL := /bin/sh
COMPOSE := docker compose

-include .env

KAFKA_HOST_PORT ?= 9092
KAFKA_UI_PORT ?= 8083
FLINK_UI_PORT ?= 8081
SPARK_MASTER_UI_PORT ?= 8082
AIRFLOW_WEB_PORT ?= 8080
POSTGRES_AIRFLOW_PORT ?= 5432
POSTGRES_SOURCE_PORT ?= 5433
CLICKHOUSE_HTTP_PORT ?= 8123
CLICKHOUSE_NATIVE_PORT ?= 9009
SUPERSET_PORT ?= 8088
NESSIE_PORT ?= 19120
NESSIE_DB_NAME ?= nessie_metadata
MINIO_API_PORT ?= 9000
MINIO_CONSOLE_PORT ?= 9001
APP ?= delivery
DATASET ?= daily-kpi
BATCH_DATE ?=
PRODUCER_APP ?= $(APP)
COMPOSE_PROJECT_NAME ?= deliveryflow

export PRODUCER_APP

.PHONY: help config build pull up down clean clean-keep-images purge restart ps status health console \
	urls url-kafka-ui url-flink url-spark url-airflow url-clickhouse url-superset url-nessie url-minio url-minio-console \
	logs logs-kafka logs-kafka-ui logs-flink logs-spark logs-airflow logs-clickhouse logs-superset logs-postgres logs-storage logs-nessie logs-producer-continuous \
	init-topics submit-flink-job import-superset-assets produce produce-stream produce-continuous stop-continuous-producer seed-batch-source spark-iceberg-test spark-daily-kpi run-batch airflow-dag-list test-e2e clean-warning docs

help:
	@echo "DeliveryFlow local platform"
	@echo "  make docs                Open the HTML Documentation Portal"
	@echo "  make config              Validate Docker Compose configuration"
	@echo "  make build               Build local Airflow, Flink, producer, tooling, Spark, and Superset images"
	@echo "  make pull                Pull pinned upstream images"
	@echo "  make up                  Start the full local platform"
	@echo "  make down                Stop containers and preserve named volumes"
	@echo "  make clean               Stop and remove project containers, preserving named volumes"
	@echo "  make clean-keep-images   Stop and remove containers/orphans, preserving volumes and images"
	@echo "  make purge               Destructively remove project containers, volumes, images, and orphans"
	@echo "  make restart             Restart containers and preserve named volumes"
	@echo "  make ps                  Show container status"
	@echo "  make health              Run non-destructive platform health checks"
	@echo "  make console             Show service endpoints"
	@echo "  make urls                Show browser URLs only"
	@echo "  make url-kafka-ui        Show Kafka UI URL"
	@echo "  make logs-kafka-ui       Tail Kafka UI logs"
	@echo "  make produce             Generate synthetic batch source rows and stream events"
	@echo "  make produce APP=fleet   Generate fleet vehicle telemetry events"
	@echo "  make produce-stream      Generate Kafka stream events only"
	@echo "  make produce-continuous  Start a background stream producer that emits 10 events every 60 seconds"
	@echo "  make stop-continuous-producer Stop the background continuous stream producer"
	@echo "  make seed-batch-source   Generate PostgreSQL batch source rows only"
	@echo "  make submit-flink-job    Submit the Kafka -> Flink -> ClickHouse job"
	@echo "  make submit-flink-job APP=fleet Submit the fleet telemetry Flink SQL job"
	@echo "  make import-superset-assets Import Superset database, datasets, charts, and dashboard from YAML"
	@echo "  make spark-iceberg-test  Validate Spark -> Nessie -> Iceberg -> S3"
	@echo "  make spark-daily-kpi     Run daily Spark KPI publication"
	@echo "  make run-batch DATASET=transportation-costs Run the Transportation Cost batch pipeline"
	@echo "  make test-e2e            Run the local end-to-end smoke test"

config:
	$(COMPOSE) config

pull:
	$(COMPOSE) pull postgres-airflow postgres-source kafka kafka-ui minio minio-init nessie clickhouse superset

build:
	$(COMPOSE) build spark-master airflow-init flink-jobmanager producer platform-tools superset

up: build
	$(COMPOSE) up -d --no-build postgres-airflow postgres-source kafka kafka-init kafka-ui minio minio-init nessie spark-master spark-worker clickhouse superset airflow-init airflow-webserver airflow-scheduler flink-jobmanager flink-taskmanager
	$(MAKE) submit-flink-job
	$(COMPOSE) up -d producer-continuous

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down --remove-orphans

clean-keep-images:
	$(COMPOSE) down --remove-orphans

purge:
	$(COMPOSE) down --volumes --remove-orphans --rmi all
	docker volume prune --all --force --filter label=com.docker.compose.project=$(COMPOSE_PROJECT_NAME)

restart:
	$(COMPOSE) restart

ps status:
	$(COMPOSE) ps

health:
	$(COMPOSE) run --rm platform-tools python /app/scripts/health_check.py

console:
	@echo Kafka external bootstrap: localhost:$(KAFKA_HOST_PORT)
	@echo Kafka internal bootstrap: kafka:9092
	@echo Kafka UI: http://localhost:$(KAFKA_UI_PORT)
	@echo Flink UI: http://localhost:$(FLINK_UI_PORT)
	@echo Spark UI: http://localhost:$(SPARK_MASTER_UI_PORT)
	@echo Airflow UI: http://localhost:$(AIRFLOW_WEB_PORT) (admin/admin by default)
	@echo PostgreSQL Airflow metadata: localhost:$(POSTGRES_AIRFLOW_PORT)
	@echo PostgreSQL ETL source: localhost:$(POSTGRES_SOURCE_PORT)
	@echo PostgreSQL Nessie metadata: postgres-source:5432/$(NESSIE_DB_NAME)
	@echo ClickHouse HTTP: http://localhost:$(CLICKHOUSE_HTTP_PORT)
	@echo ClickHouse native: localhost:$(CLICKHOUSE_NATIVE_PORT)
	@echo Superset UI: http://localhost:$(SUPERSET_PORT) (admin/admin by default)
	@echo Nessie API: http://localhost:$(NESSIE_PORT)/api/v2
	@echo MinIO API: http://localhost:$(MINIO_API_PORT)
	@echo MinIO Console: http://localhost:$(MINIO_CONSOLE_PORT)

urls:
	@echo Kafka UI: http://localhost:$(KAFKA_UI_PORT)
	@echo Flink UI: http://localhost:$(FLINK_UI_PORT)
	@echo Spark UI: http://localhost:$(SPARK_MASTER_UI_PORT)
	@echo Airflow UI: http://localhost:$(AIRFLOW_WEB_PORT)
	@echo ClickHouse HTTP: http://localhost:$(CLICKHOUSE_HTTP_PORT)
	@echo Superset UI: http://localhost:$(SUPERSET_PORT)
	@echo Nessie API: http://localhost:$(NESSIE_PORT)/api/v2
	@echo MinIO API: http://localhost:$(MINIO_API_PORT)
	@echo MinIO Console: http://localhost:$(MINIO_CONSOLE_PORT)

url-kafka-ui:
	@echo http://localhost:$(KAFKA_UI_PORT)

url-flink:
	@echo http://localhost:$(FLINK_UI_PORT)

url-spark:
	@echo http://localhost:$(SPARK_MASTER_UI_PORT)

url-airflow:
	@echo http://localhost:$(AIRFLOW_WEB_PORT)

url-clickhouse:
	@echo http://localhost:$(CLICKHOUSE_HTTP_PORT)

url-superset:
	@echo http://localhost:$(SUPERSET_PORT)

url-nessie:
	@echo http://localhost:$(NESSIE_PORT)/api/v2

url-minio:
	@echo http://localhost:$(MINIO_API_PORT)

url-minio-console:
	@echo http://localhost:$(MINIO_CONSOLE_PORT)

logs:
	$(COMPOSE) logs -f

logs-kafka:
	$(COMPOSE) logs -f kafka kafka-init

logs-kafka-ui:
	$(COMPOSE) logs -f kafka-ui

logs-flink:
	$(COMPOSE) logs -f flink-jobmanager flink-taskmanager flink-job-submit

logs-spark:
	$(COMPOSE) logs -f spark-master spark-worker

logs-airflow:
	$(COMPOSE) logs -f airflow-webserver airflow-scheduler airflow-init

logs-clickhouse:
	$(COMPOSE) logs -f clickhouse

logs-superset:
	$(COMPOSE) logs -f superset

logs-postgres:
	$(COMPOSE) logs -f postgres-airflow postgres-source

logs-storage:
	$(COMPOSE) logs -f minio minio-init

logs-nessie:
	$(COMPOSE) logs -f nessie

logs-producer-continuous:
	$(COMPOSE) logs -f producer-continuous

init-topics:
	$(COMPOSE) run --rm kafka-init

submit-flink-job:
ifeq ($(APP),fleet)
	$(COMPOSE) build flink-jobmanager
	$(COMPOSE) run --rm flink-job-submit-fleet
else
	$(COMPOSE) run --rm flink-job-submit
endif

import-superset-assets:
	$(COMPOSE) build superset-importer
	$(COMPOSE) run --rm superset-importer

produce:
ifeq ($(APP),fleet)
	$(COMPOSE) run --rm -e PRODUCER_APP=fleet -e PRODUCER_MODE=stream producer python -m producers.synthetic_logistics_producer
else
	$(COMPOSE) run --rm producer python -m producers.synthetic_logistics_producer
endif

produce-stream:
	$(COMPOSE) run --rm -e PRODUCER_MODE=stream producer python -m producers.synthetic_logistics_producer

produce-continuous:
	$(COMPOSE) up -d producer-continuous

stop-continuous-producer:
	$(COMPOSE) stop producer-continuous

seed-batch-source:
	$(COMPOSE) run --rm -e PRODUCER_MODE=batch producer python -m producers.synthetic_logistics_producer

spark-iceberg-test:
	$(COMPOSE) exec -T -u 0 spark-master /opt/spark/bin/spark-submit /opt/deliveryflow/src/etl/apps/iceberg_smoke_test.py

spark-daily-kpi:
	$(COMPOSE) exec -T -u 0 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/deliveryflow/src/etl/apps/daily_kpi_job.py

run-batch:
ifeq ($(DATASET),transportation-costs)
	$(COMPOSE) exec -T -u 0 -e BATCH_DATE=$(BATCH_DATE) spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/deliveryflow/src/etl/apps/transportation_cost_kpi_job.py
else
	$(COMPOSE) exec -T -u 0 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/deliveryflow/src/etl/apps/daily_kpi_job.py
endif

airflow-dag-list:
	$(COMPOSE) exec airflow-scheduler airflow dags list

test-e2e:
ifeq ($(APP),fleet)
	$(COMPOSE) run --rm -e E2E_APP=fleet platform-tools python /app/scripts/e2e_smoke_test.py
else
	$(COMPOSE) run --rm platform-tools python /app/scripts/e2e_smoke_test.py
endif

clean-warning:
	@echo "Destructive cleanup is intentionally not implemented as a default target."
	@echo "Do not remove named volumes without explicit approval."

docs:
	@echo "Opening DeliveryFlow Documentation Portal..."
	@python -c "import webbrowser, os; webbrowser.open('file://' + os.path.abspath('docs/index.html'))" 2>/dev/null || echo "Documentation Portal located at: docs/index.html"
