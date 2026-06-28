-- Create PostgreSQL tables for module 03 window exercises
-- Run: make db-query SQL="$(cat exercises/03-windows/setup_tables.sql)"
-- Or: uvx pgcli -h localhost -U postgres -d postgres < exercises/03-windows/setup_tables.sql

CREATE TABLE IF NOT EXISTS minute_revenue (
    zone_id INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    trip_count BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (zone_id, window_start)
);

CREATE TABLE IF NOT EXISTS rolling_avg_fare (
    zone_id INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    trip_count BIGINT,
    avg_fare DOUBLE PRECISION,
    PRIMARY KEY (zone_id, window_start)
);

CREATE TABLE IF NOT EXISTS driver_sessions (
    driver_id INTEGER,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    trip_count BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (driver_id, session_start)
);
