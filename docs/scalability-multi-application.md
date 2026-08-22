# DeliveryFlow Scalability and Multi-Application Architecture

This document records the implementation decisions from `DeliveryFlow_Scalability_and_Flink_SQL_Prompts.md`. The goal is not to make the local lab unnecessarily distributed; the goal is to stop the project from growing into one custom script, one custom DAG, one custom job, and one manual dashboard definition per table.

## Target Shape

DeliveryFlow now separates platform code from application metadata.

```text
configs/applications/<app>.yaml     application ownership, topics, jobs, thresholds
configs/datasets/<domain>/*.yaml    dataset contract, storage, serving metadata
src/contracts/<domain>/*.json       versioned JSON event contracts
services/flink/...                  reusable stream application code
services/clickhouse/init/...        serving schemas and databases
```

This gives the project a control-plane style structure for future scale:

- many applications can be described without putting every setting in `.env`;
- each domain owns its own contracts and datasets;
- runtime jobs can be selected with `APP=<name>`;
- topic and ClickHouse ownership is explicit;
- application additions do not require modifying the existing delivery job.

## Current Applications

### Delivery Operations

Delivery operations remains the default application.

```text
Producer -> Kafka delivery-events -> DeliveryStreamingJob -> ClickHouse delivery.*
```

Important assets:

- application config: `configs/applications/delivery.yaml`
- event contract: `src/contracts/delivery/delivery_event_v1.schema.json`
- Flink job: `services/flink/src/main/java/local/deliveryflow/DeliveryStreamingJob.java`
- serving database: `delivery`
- BI import file: `configs/superset/deliveryflow_bi.yaml`

### Fleet Vehicle Telemetry

Fleet telemetry is the second streaming application. It is intentionally isolated from the delivery DataStream job.

```text
Producer APP=fleet -> Kafka vehicle-telemetry-events -> VehicleTelemetrySqlJob -> ClickHouse fleet.*
```

Important assets:

- application config: `configs/applications/fleet.yaml`
- dataset config: `configs/datasets/fleet/vehicle_telemetry.yaml`
- event contract: `src/contracts/fleet/vehicle_telemetry_v1.schema.json`
- Flink SQL/Table API job: `services/flink/src/main/java/local/deliveryflow/VehicleTelemetrySqlJob.java`
- serving database: `fleet`

Fleet output tables:

- `fleet.vehicle_telemetry_events`: immutable telemetry history.
- `fleet.vehicle_current_state`: latest known state per vehicle.
- `fleet.vehicle_health_alerts`: overspeed, low-fuel, and engine-temperature alerts.
- `fleet.vehicle_metrics_5m`: five-minute operational metrics.

## Operator Commands

Default delivery flow:

```powershell
make submit-flink-job
make produce
make test-e2e
```

Fleet telemetry flow:

```powershell
make submit-flink-job APP=fleet
make produce APP=fleet
make test-e2e APP=fleet
```

The same `Makefile` entrypoints are used because the application identity is runtime metadata, not a separate local workflow.

## Anti-Patterns Avoided

The new structure avoids these growth problems:

- one `.env` containing every future table setting;
- one Kafka topic per table by default;
- one Flink job per table by default;
- one Superset YAML for every domain;
- hardcoded schemas without versioned contract files;
- mixing delivery and fleet serving tables in the same ownership namespace.

## Scale Guidance

When adding the next application, follow this order:

1. Add or update `configs/applications/<app>.yaml`.
2. Add dataset metadata under `configs/datasets/<domain>/`.
3. Add versioned contracts under `src/contracts/<domain>/`.
4. Reuse an existing producer/job pattern where possible.
5. Add ClickHouse database/table DDL under `services/clickhouse/init/`.
6. Add a Makefile `APP=<name>` route only when the app has a distinct runtime.
7. Add tests for contract generation and the smoke path.

This keeps local development simple while preparing the repository for many domains, many datasets, and many operational workloads.
