# Superset Serving Strategy

## Current BI Scope

Superset BI-as-code currently covers the `delivery` application dashboard and its five approved statistics. The new `fleet` stream application writes serving tables into ClickHouse `fleet.*`, but no fleet Superset dashboard is imported yet.

This separation is intentional:

- `delivery` BI assets stay in `configs/superset/deliveryflow_bi.yaml`.
- `fleet` serving tables are ready for future BI assets.
- Future domains should use separate BI metadata instead of putting every dashboard into one YAML file.

Fleet tables available for future Superset datasets:

- `fleet.vehicle_telemetry_events`
- `fleet.vehicle_current_state`
- `fleet.vehicle_health_alerts`
- `fleet.vehicle_metrics_5m`

Bu sənəd DeliveryFlow layihəsində BI serving strategiyasını izah edir. Layihədə BI və dashboard qatı üçün Apache Superset istifadə olunur. Superset Docker Compose daxilində işləyir və ClickHouse-dakı hazır serving cədvəllərini və view-ları oxuyur.

## Niyə Superset?

Superset bu lokal lab üçün uyğundur, çünki:

- Docker Compose daxilində işləyə bilir.
- ClickHouse ilə birbaşa işləyə bilir.
- Dashboard, chart və dataset anlayışlarını ayrıca verir.
- Stream və batch nəticələrini eyni UI-da göstərmək olur.
- Lokal development üçün əlavə desktop BI tool tələb etmir.

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

ClickHouse connection üçün SQLAlchemy URI:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

## Superset-in Oxuduğu Data Qatı

Superset Kafka-dan oxumamalıdır. Superset raw object storage fayllarını da oxumamalıdır. Bu layihədə Superset üçün approved serving layer ClickHouse-dur.

Əsas səbəb:

- Kafka event stream-dir, dashboard üçün sorğu engine deyil.
- MinIO/Iceberg analytical storage-dur, birbaşa dashboard latency-si üçün seçilməyib.
- ClickHouse columnar serving DB-dir və dashboard sorğuları üçün daha uyğundur.

## Dataset Mənbələri

Superset-də dataset kimi bu table və view-lar istifadə olunmalıdır.

Əsas cədvəllər:

- `delivery.delivery_events`
- `delivery.delivery_current_state`
- `delivery.vehicle_current_state`
- `delivery.daily_delivery_kpi`

Hazır report view-lar:

- `delivery.v_delivery_status_overview`
- `delivery.v_delay_by_region`
- `delivery.v_vehicle_utilization`
- `delivery.v_warehouse_daily_kpi`
- `delivery.v_delivery_event_volume`

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

## Dashboard Dizaynı

Superset dashboard-u 5 əsas statistik hesabat üzərində qurulmalıdır. Bu dashboard operational və analytical suallara cavab verməlidir:

- Hazırda delivery status paylanması necədir?
- Hansı region və service level gecikməyə daha çox təsir edir?
- Vehicle utilization normaldırmı?
- Warehouse-lar üzrə daily KPI necə dəyişir?
- Event volume saatlara görə necə paylanır?

## Hesabat 1: Delivery Status Overview

Dataset:

```text
delivery.v_delivery_status_overview
```

Məqsəd:

Statuslara görə event və delivery sayını göstərir. Bu chart operational vəziyyəti tez anlamaq üçündür.

Tövsiyə olunan chart:

- Bar Chart

Metrics:

- `event_count`
- `delivery_count`
- `avg_delay_minutes`

Dimensions:

- `status`

Dashboard-da cavab verdiyi suallar:

- Ən çox hansı status gəlir?
- Gecikmə hansı statuslarda daha çox görünür?
- Delivered və delayed event-lərin nisbəti normaldırmı?

## Hesabat 2: Delay by Region and Service Level

Dataset:

```text
delivery.v_delay_by_region
```

Məqsəd:

Region və service level üzrə gecikmə davranışını göstərir.

Tövsiyə olunan chart:

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

Dashboard-da cavab verdiyi suallar:

- Hansı region gecikir?
- Express sifarişlər standard sifarişlərdən daha gec çatırmı?
- Region və service level birlikdə risk yaradırmı?

## Hesabat 3: Vehicle Utilization

Dataset:

```text
delivery.v_vehicle_utilization
```

Məqsəd:

Vehicle-ların son vəziyyətini və utilization səviyyəsini göstərir.

Tövsiyə olunan chart:

- Table
- Bar Chart
- Big Number with Trend, əgər ayrıca filtr istifadə edilirsə

Metrics:

- `avg_utilization_ratio`
- `last_seen_at`

Dimensions:

- `vehicle_id`
- `active_status`
- `route_id`

Dashboard-da cavab verdiyi suallar:

- Hansı vehicle daha çox yüklənib?
- Hansı vehicle sonuncu dəfə nə vaxt event göndərib?
- Həddindən artıq və ya az istifadə olunan vehicle varmı?

## Hesabat 4: Warehouse Daily KPI

Dataset:

```text
delivery.v_warehouse_daily_kpi
```

Məqsəd:

Warehouse, region və tarix səviyyəsində daily KPI-ları göstərir. Bu chart batch pipeline nəticəsini yoxlamaq üçün əsas yerdir.

Tövsiyə olunan chart:

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

Dashboard-da cavab verdiyi suallar:

- Günlük completed delivery sayı artırmı?
- Delay rate hansı warehouse-da yüksəkdir?
- Order value və package count hansı regionda daha çoxdur?
- Vehicle utilization warehouse performansı ilə uyğun gəlirmi?

## Hesabat 5: Delivery Event Volume

Dataset:

```text
delivery.v_delivery_event_volume
```

Məqsəd:

Saatlıq event volume-u göstərir. Bu chart streaming pipeline-ın işləyib-işləmədiyini vizual yoxlamağa kömək edir.

Tövsiyə olunan chart:

- Time-series Line Chart
- Stacked Bar Chart

Metrics:

- `event_count`
- `delivery_count`

Dimensions:

- `event_hour`
- `event_type`
- `region`

Dashboard-da cavab verdiyi suallar:

- Event-lər saatlara görə stabil gəlirmi?
- Hansı event type daha çoxdur?
- Region üzrə event yükü necə bölünür?

## Dashboard Layout Təklifi

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

## Superset Setup Addımları

1. Platformanı başladın:

```powershell
make up
```

2. Superset aç:

```text
http://localhost:8088
```

3. Database connection yarat:

```text
clickhousedb://delivery_app:local-clickhouse-password@clickhouse:8123/delivery
```

4. Superset obyektlərini BI-as-code import et:

```powershell
make import-superset-assets
```

Bu command `configs/superset/deliveryflow_bi.yaml` faylından database, dataset, chart və dashboard obyektlərini yaradır və ya yeniləyir.

5. Dataset-lərin yarandığını yoxla:

```text
v_delivery_status_overview
v_delay_by_region
v_vehicle_utilization
v_warehouse_daily_kpi
v_delivery_event_volume
```

6. Dashboard-u aç:

```text
http://localhost:8088/superset/dashboard/deliveryflow-operations/
```

## Debug və Yoxlama

ClickHouse-da view-ları yoxlamaq:

```powershell
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM delivery"
```

Bir view-dan sample oxumaq:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM delivery.v_delay_by_region LIMIT 10"
```

Superset log-ları:

```powershell
make logs-superset
```

Superset URL:

```powershell
make url-superset
```

## Vacib Qeyd

ClickHouse init SQL yalnız yeni volume yaradıldıqda avtomatik işləyir. Əgər köhnə volume qalırsa və yeni view-lar görünmürsə, destructive reset lazımdır:

```powershell
make purge
make up
```

`make purge` named volume-ları sildiyi üçün lokal data da silinir.
## BI as Code Modeli

Superset obyektleri UI-da elle klikle yaradilmaga mecbur deyil. Bu repo Superset obyektlerini YAML fayli ile saxlayir:

```text
configs/superset/deliveryflow_bi.yaml
```

Bu YAML fayli asagidaki obyektler ucun source of truth rolunu oynayir:

- database: `DeliveryFlow ClickHouse`
- datasets: 5 ClickHouse report view-u
- charts: 5 operational/statistical report
- dashboard: `DeliveryFlow Operations Dashboard`
- temporal metadata: `business_date` ve `event_hour`
- chart metric metadata: Superset adhoc metric formatina cevrilen YAML metric adlari

Import script:

```text
scripts/import_superset_assets.py
```

Import command:

```powershell
make import-superset-assets
```

Script Superset REST API ile login olur, CSRF token alir ve YAML-daki obyektleri idempotent formada yaradir ve ya yenileyir. `make import-superset-assets` evvel `superset-importer` image-ini rebuild edir ki, script ve YAML deyisiklikleri container daxilinde kohne qalmasin.

Importer chart parametrlerini de normallasdirir:

- `event_count`, `completed_deliveries`, `total_order_value` kimi numeric column-lar chart daxilinde `SUM(...)` adhoc metric kimi yazilir.
- `delay_rate`, `on_time_rate`, `avg_delay_minutes`, `avg_utilization_ratio` kimi rate/average column-lar `AVG(...)` adhoc metric kimi yazilir.
- `v_delivery_event_volume.event_hour` dataset-de temporal column ve main datetime column kimi saxlanilir.
- `Hourly Delivery Event Volume` chart-i `x_axis: event_hour`, `granularity_sqla: event_hour`, `time_grain_sqla: PT1H` ile import olunur.

Bu qayda asagidaki Superset render xetalarinin qarsisini alir:

```text
Metric 'event_count' does not exist
Metric 'delay_rate' does not exist
Datetime column not provided as part table configuration and is required by this type of chart
```

Superset-in native import/export endpoint-leri de bu strategiyaya uygundur:

- dashboard import: `POST /api/v1/dashboard/import/`
- dataset import: `POST /api/v1/dataset/import/`
- assets import: `POST /api/v1/assets/import/`

Bu layihedeki `make import-superset-assets` daha sade local workflow ucun YAML-dan REST create/update edir. Gelecekde eyni YAML spec native Superset ZIP export formatina cevrilib `/api/v1/assets/import/` endpoint-ine baglana biler.
