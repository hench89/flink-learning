-- Create PostgreSQL tables for module 04 state exercises
-- Run: make db-setup FILE=exercises/04-state/setup_tables.sql

CREATE TABLE IF NOT EXISTS zone_trip_totals (
    zone_id INTEGER PRIMARY KEY,
    total_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS zone_max_fare (
    zone_id INTEGER PRIMARY KEY,
    max_fare DOUBLE PRECISION,
    max_distance DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS zone_cumulative (
    zone_id INTEGER PRIMARY KEY,
    total_trips BIGINT
);

CREATE TABLE IF NOT EXISTS zone_rate_alerts (
    zone_id INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    ride_count BIGINT,
    PRIMARY KEY (zone_id, window_start)
);
