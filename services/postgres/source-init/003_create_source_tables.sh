#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SOURCE_DB_NAME" <<SQL
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id TEXT PRIMARY KEY,
    warehouse_name TEXT NOT NULL,
    region TEXT NOT NULL,
    city TEXT NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id TEXT PRIMARY KEY,
    plate_number TEXT NOT NULL UNIQUE,
    vehicle_type TEXT NOT NULL,
    capacity_total NUMERIC(12, 2) NOT NULL CHECK (capacity_total > 0),
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    phone_number TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customer_orders (
    order_id TEXT PRIMARY KEY,
    customer_region TEXT NOT NULL,
    destination_latitude NUMERIC(9, 6) NOT NULL,
    destination_longitude NUMERIC(9, 6) NOT NULL,
    order_created_at TIMESTAMPTZ NOT NULL,
    promised_delivery_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (promised_delivery_at > order_created_at)
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES customer_orders(order_id),
    warehouse_id TEXT NOT NULL REFERENCES warehouses(warehouse_id),
    shipment_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS delivery_plans (
    delivery_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL REFERENCES shipments(shipment_id),
    vehicle_id TEXT NOT NULL REFERENCES vehicles(vehicle_id),
    driver_id TEXT NOT NULL REFERENCES drivers(driver_id),
    route_id TEXT NOT NULL,
    planned_departure_at TIMESTAMPTZ NOT NULL,
    planned_arrival_at TIMESTAMPTZ NOT NULL,
    planned_distance_km NUMERIC(12, 2) NOT NULL CHECK (planned_distance_km >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (planned_arrival_at > planned_departure_at)
);

CREATE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_warehouse_id ON shipments(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_delivery_plans_shipment_id ON delivery_plans(shipment_id);
CREATE INDEX IF NOT EXISTS idx_delivery_plans_vehicle_id ON delivery_plans(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_delivery_plans_driver_id ON delivery_plans(driver_id);
CREATE INDEX IF NOT EXISTS idx_customer_orders_promised_delivery_at ON customer_orders(promised_delivery_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${SOURCE_DB_USER};
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${SOURCE_DB_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${SOURCE_DB_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO ${SOURCE_DB_USER};
SQL
