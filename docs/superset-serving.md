# Superset Serving Strategy

## Current BI Scope

Superset BI-as-code currently covers the `delivery` application dashboard and its five approved statistics. The new `fleet` stream application writes serving tables into ClickHouse `fleet.*`, but no fleet Superset dashboard is imported yet.
Superset BI-as-code currently covers two production dashboards for the `delivery` application:
1. **DeliveryFlow Operations Dashboard** (5 operational stream & batch charts)
2. **Transportation Cost & Route Performance Dashboard** (7 financial & route risk charts with 4 native filters)

The `fleet` stream application writes serving tables into ClickHouse `fleet.*`, ready for future telemetry dashboards.

This separation is intentional:

- `delivery` BI assets stay in `configs/superset/deliveryflow_bi.yaml`.
- `delivery` BI assets are defined declaratively in `configs/superset/deliveryflow_bi.yaml`.
- `fleet` serving tables are ready for future BI assets.
- Future domains should use separate BI metadata instead of putting every dashboard into one YAML file.
- Future domains use separate BI metadata instead of putting every dashboard into one YAML file.

Fleet tables available for future Superset datasets:

- `fleet.vehicle_telemetry_events`
- `fleet.vehicle_current_state`
- `fleet.vehicle_health_alerts`
- `fleet.vehicle_metrics_5m`

This document explains the BI serving strategy for DeliveryFlow. Apache Superset provides the BI and dashboard layer. Superset runs within Docker Compose and reads prepared serving tables and views from ClickHouse.

## Why Superset?

Superset fits this local lab because it:

- Runs within Docker Compose.
- Works directly with ClickHouse.
- Separates dashboards, charts, and datasets.
- Can display stream and batch results in the same UI.
- Does not require an additional desktop BI tool for local development.

```mermaid
flowchart LR
    flink["Flink Stream Job"]
    spark["Spark Batch Job"]
    clickhouse["ClickHouse<br/>Serving Database"]
    views["Prepared Report Views"]
    superset["Apache Superset"]
    users["Data / Business User"]

    flink -->|operational state| clickhouse
    spark -->|daily KPI| clickhouse
    clickhouse --> views
    views --> superset
    superset --> users
```

## Local Access

Superset URL:

```text
http://localhost:8088
```

Default local login:

```text
username: admin
password: admin
```

SQLAlchemy URI for the ClickHouse connection:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

## Superset Data Layer

Superset should not read from Kafka or raw object-storage files. In this project, the approved serving layer for Superset is ClickHouse.

Main reasons:

- Kafka is an event stream, not a dashboard query engine.
- MinIO/Iceberg provide analytical storage and are not selected for direct dashboard latency.
- ClickHouse is a columnar serving database better suited to dashboard queries.

## Dataset Sources

These tables and views should be used as Superset datasets.

Primary tables:

- `delivery.delivery_events`
- `delivery.delivery_current_state`
- `delivery.vehicle_current_state`
- `delivery.daily_delivery_kpi`
- `delivery.daily_transportation_cost_kpi`

Prepared report views:

- `delivery.v_delivery_status_overview`
- `delivery.v_delay_by_region`
- `delivery.v_vehicle_utilization`
- `delivery.v_warehouse_daily_kpi`
- `delivery.v_delivery_event_volume`
- `delivery.v_transportation_cost_performance`

```mermaid
flowchart TB
    subgraph tables["ClickHouse Base Tables"]
        events["delivery_events"]
        deliveryState["delivery_current_state"]
        vehicleState["vehicle_current_state"]
        dailyKpi["daily_delivery_kpi"]
    end

    subgraph reportViews["BI-Ready Views"]
        statusOverview["v_delivery_status_overview"]
        delayRegion["v_delay_by_region"]
        vehicleUtil["v_vehicle_utilization"]
        warehouseKpi["v_warehouse_daily_kpi"]
        eventVolume["v_delivery_event_volume"]
    end

    subgraph dashboard["Superset Dashboard"]
        chart1["Status Overview"]
        chart2["Delay by Region"]
        chart3["Vehicle Utilization"]
        chart4["Warehouse KPI"]
        chart5["Event Volume"]
    end

    events --> statusOverview
    events --> delayRegion
    vehicleState --> vehicleUtil
    dailyKpi --> warehouseKpi
    events --> eventVolume

    statusOverview --> chart1
    delayRegion --> chart2
    vehicleUtil --> chart3
    warehouseKpi --> chart4
    eventVolume --> chart5
```

## Dashboard Design

The Superset dashboard should contain five primary statistical reports. It should answer operational and analytical questions such as:

- What is the current delivery-status distribution?
- Which region and service level contribute most to delays?
- Is vehicle utilization within the expected range?
- How do daily KPIs change by warehouse?
- How is event volume distributed by hour?

## Report 1: Delivery Status Overview

Dataset:

```text
delivery.v_delivery_status_overview
```

Purpose:

Shows event and delivery counts by status. This chart provides a quick view of the operational state.

Recommended chart:

- Bar Chart

Metrics:

- `event_count`
- `delivery_count`
- `avg_delay_minutes`

Dimensions:

- `status`

Questions answered by the dashboard:

- Which status occurs most often?
- Which statuses show the most delay?
- Is the ratio of delivered to delayed events within the expected range?

## Report 2: Delay by Region and Service Level

Dataset:

```text
delivery.v_delay_by_region
```

Purpose:

Shows delay behavior by region and service level.

Recommended chart:

- Heatmap
- Pivot Table
- Grouped Bar Chart

Metrics:

- `delay_rate`
- `avg_delay_minutes`
- `event_count`

Dimensions:

- `region`
- `service_level`

Questions answered by the dashboard:

- Which region is delayed?
- Do express orders arrive later than standard orders?
- Do region and service level create a combined risk?

## Report 3: Vehicle Utilization

Dataset:

```text
delivery.v_vehicle_utilization
```

Purpose:

Shows the latest vehicle state and utilization level.

Recommended chart:

- Table
- Bar Chart
- Big Number with Trend, when a separate filter is used

Metrics:

- `avg_utilization_ratio`
- `last_seen_at`

Dimensions:

- `vehicle_id`
- `active_status`
- `route_id`

Questions answered by the dashboard:

- Which vehicle is carrying the highest load?
- When did each vehicle last send an event?
- Are any vehicles over- or under-utilized?

## Report 4: Warehouse Daily KPI

Dataset:

```text
delivery.v_warehouse_daily_kpi
```

Purpose:

Shows daily KPIs by warehouse, region, and date. This chart is the main place to verify batch pipeline results.

Recommended chart:

- Time-series Line Chart
- Table
- Mixed Chart

Metrics:

- `completed_deliveries`
- `delayed_deliveries`
- `on_time_deliveries`
- `delay_rate`
- `on_time_rate`
- `avg_vehicle_utilization`
- `total_order_value`
- `total_package_count`
- `avg_distance_km`

Dimensions:

- `business_date`
- `region`
- `warehouse_id`

Questions answered by the dashboard:

- Is the daily completed-delivery count increasing?
- Which warehouse has the highest delay rate?
- Which region has the highest order value and package count?
- Is vehicle utilization consistent with warehouse performance?

## Report 5: Delivery Event Volume

Dataset:

```text
delivery.v_delivery_event_volume
```

Purpose:

Shows hourly event volume. This chart helps visually verify that the streaming pipeline is working.

Recommended chart:

- Time-series Line Chart
- Stacked Bar Chart

Metrics:

- `event_count`
- `delivery_count`

Dimensions:

- `event_hour`
- `event_type`
- `region`

Questions answered by the dashboard:

- Do events arrive at a stable rate by hour?
- Which event type is most common?
- How is event load distributed by region?

## Dashboard 2: Transportation Cost & Route Performance

Dataset: `delivery.v_transportation_cost_performance`

Filters: `business_date`, `warehouse_id`, `region`, `route_id`

### Report 6: Actual Transportation Cost
- Visualization: Big Number (`big_number_total`)
- Metric: `actual_cost` (SUM)
- Subheader: "Actual transportation cost"
- Purpose: Displays total operational transportation spend in dollars.

### Report 7: Transportation Cost Variance
- Visualization: Big Number (`big_number_total`)
- Metric: `cost_variance` (SUM)
- Subheader: "Actual cost minus planned cost"
- Purpose: Tracks absolute budget deviation across routes.

### Report 8: Transportation Cost per Delivery
- Visualization: Big Number (`big_number_total`)
- Metric: `cost_per_delivery` (AVG)
- Subheader: "Actual cost per delivery"
- Purpose: Highlights unit drop-off economics.

### Report 9: Transportation Cost Breakdown
- Visualization: Stacked Bar (`dist_bar`)
- Metrics: `fuel_cost`, `driver_cost`, `toll_cost`, `maintenance_cost`, `other_cost`
- Dimension: `business_date`
- Purpose: Analyzes daily cost composition shares.

### Report 10: Route Cost Performance
- Visualization: Table (`table`)
- Metrics: `actual_cost`, `cost_per_delivery`, `cost_variance`
- Dimensions: `route_id`, `region`
- Purpose: Ranks transit corridors by actual cost and budget overruns.

### Report 11: Warehouse Region Transportation Cost
- Visualization: Distribution Bar (`dist_bar`)
- Metric: `actual_cost`
- Dimensions: `warehouse_id`, `region`
- Purpose: Identifies high-cost warehouse hubs and delivery territories.

### Report 12: Route Cost vs Delay Risk
- Visualization: Table (`table`)
- Metrics: `actual_cost`, `delay_rate`, `avg_delay_minutes`, `cost_delay_risk_score`
- Dimensions: `route_id`, `warehouse_id`, `region`
- Purpose: Flags high-cost routes that simultaneously suffer from excessive shipment delays.

## Dashboard Layout Proposal

```mermaid
flowchart TB
    title["DeliveryFlow Operations Dashboard"]
    row1a["Big Number<br/>Total Deliveries"]
    row1b["Big Number<br/>Delay Rate"]
    row1c["Big Number<br/>Avg Utilization"]
    row2a["Bar<br/>Status Overview"]
    row2b["Heatmap<br/>Delay by Region"]
    row3a["Line<br/>Hourly Event Volume"]
    row3b["Table<br/>Vehicle Utilization"]
    row4["Table / Line<br/>Warehouse Daily KPI"]

    title --> row1a
    title --> row1b
    title --> row1c
    row1a --> row2a
    row1b --> row2b
    row2a --> row3a
    row2b --> row3b
    row3a --> row4
    row3b --> row4
```

## Superset Setup Steps

1. Start the platform:

```powershell
make up
```

2. Open Superset:

```text
http://localhost:8088
```

3. Create the database connection:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

4. Import the Superset objects as BI-as-code:

```powershell
make import-superset-assets
```

This command creates or updates database, dataset, chart, and dashboard objects from `configs/superset/deliveryflow_bi.yaml`.

5. Verify that the datasets were created:

```text
v_delivery_status_overview
v_delay_by_region
v_vehicle_utilization
v_warehouse_daily_kpi
v_delivery_event_volume
```

6. Open the dashboard:

```text
http://localhost:8088/superset/dashboard/deliveryflow-operations/
```

## Debugging and Verification

To verify views in ClickHouse:

```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

To read a sample from a view:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM delivery.v_delay_by_region LIMIT 10"
```

Superset logs:

```powershell
make logs-superset
```

Superset URL:

```powershell
make url-superset
```

## Important Note

ClickHouse init SQL runs automatically only when a new volume is created. If an old volume remains and new views are missing, a destructive reset is required:

```powershell
make purge
make up
```

`make purge` also removes local data because it deletes named volumes.
## BI-as-Code Model

Superset objects do not need to be created by clicking through the UI. This repository stores them in a YAML file:

```text
configs/superset/deliveryflow_bi.yaml
```

This YAML file is the source of truth for the following objects:

- database: `DeliveryFlow ClickHouse`
- datasets: 5 ClickHouse report views
- charts: 5 operational/statistical report
- dashboard: `DeliveryFlow Operations Dashboard`
- datasets: 6 ClickHouse report views (including `v_transportation_cost_performance`)
- charts: 12 operational, financial, and statistical charts
- dashboards: 2 dashboards (`DeliveryFlow Operations Dashboard` and `Transportation Cost & Route Performance`)
- native filters: `business_date`, `warehouse_id`, `region`, and `route_id`
- temporal metadata: `business_date` and `event_hour`
- chart metric metadata: YAML metric names converted to Superset adhoc metric format

Import script:

```text
scripts/import_superset_assets.py
```

Import command:

```powershell
make import-superset-assets
```

The script logs in to the Superset REST API, obtains a CSRF token, and creates or updates the YAML objects idempotently. `make import-superset-assets` rebuilds the `superset-importer` image first so script and YAML changes are not stale inside the container.

The importer also normalizes chart parameters:

- Numeric columns such as `event_count`, `completed_deliveries`, and `total_order_value` are written as `SUM(...)` adhoc metrics in the chart.
- Rate or average columns such as `delay_rate`, `on_time_rate`, `avg_delay_minutes`, and `avg_utilization_ratio` are written as `AVG(...)` adhoc metrics.
- `v_delivery_event_volume.event_hour` is stored as the dataset temporal and main datetime column.
- The `Hourly Delivery Event Volume` chart is imported with `x_axis: event_hour`, `granularity_sqla: event_hour`, and `time_grain_sqla: PT1H`.

This rule prevents the following Superset rendering errors:

```text
Metric 'event_count' does not exist
Metric 'delay_rate' does not exist
Datetime column not provided as part table configuration and is required by this type of chart
```

Superset native import/export endpoints also support this strategy:

- dashboard import: `POST /api/v1/dashboard/import/`
- dataset import: `POST /api/v1/dataset/import/`
- assets import: `POST /api/v1/assets/import/`

In this project, `make import-superset-assets` performs REST create/update from YAML for a simpler local workflow. In the future, the same YAML specification could be converted to the native Superset ZIP export format and sent to the `/api/v1/assets/import/` endpoint.
