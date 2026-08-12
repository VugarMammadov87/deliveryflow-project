# Power BI Serving Preparation

Power BI is not deployed as a Docker Compose service. It should connect to the approved serving layer.

## Operational Dataset

Use ClickHouse database `delivery` through host access:

- HTTP endpoint: `http://localhost:8123`
- Native endpoint: `localhost:9009`
- User: `delivery_app`
- Password: local development value from `.env`

Recommended operational tables:

- `delivery.delivery_current_state`
- `delivery.vehicle_current_state`
- `delivery.delivery_events`

## Management Dataset

Recommended analytical table:

- `delivery.daily_delivery_kpi`

The source of truth for historical tables remains Iceberg in object storage, cataloged by Nessie. ClickHouse is the serving layer for low-latency Power BI access in this local environment.
