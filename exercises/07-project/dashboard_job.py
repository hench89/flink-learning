"""Capstone: Multi-output dashboard job combining windows, state, and watermarks."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # === SOURCE ===
    t_env.execute_sql("""
        CREATE TABLE rides (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            tpep_pickup_datetime BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'rides',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # === SINK 1: Hourly revenue (tumbling window) ===
    t_env.execute_sql("""
        CREATE TABLE dashboard_hourly_revenue (
            zone_id INTEGER,
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            revenue DOUBLE,
            PRIMARY KEY (zone_id, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'dashboard_hourly_revenue',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # === SINK 2: Top zones by total trips (stateful aggregation) ===
    t_env.execute_sql("""
        CREATE TABLE dashboard_top_zones (
            zone_id INTEGER,
            total_trips BIGINT,
            total_revenue DOUBLE,
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'dashboard_top_zones',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # === SINK 3: Average trip stats (keyed state) ===
    t_env.execute_sql("""
        CREATE TABLE dashboard_avg_stats (
            zone_id INTEGER,
            avg_fare DOUBLE,
            avg_distance DOUBLE,
            trip_count BIGINT,
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'dashboard_avg_stats',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # === SINK 4: Processing stats (monitoring) ===
    t_env.execute_sql("""
        CREATE TABLE dashboard_processing_stats (
            stat_name STRING,
            stat_value BIGINT,
            last_updated TIMESTAMP(3),
            PRIMARY KEY (stat_name) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'dashboard_processing_stats',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # === EXECUTE MULTI-SINK JOB ===
    stmt_set = t_env.create_statement_set()

    # Output 1: Hourly revenue (window)
    stmt_set.add_insert_sql("""
        INSERT INTO dashboard_hourly_revenue
        SELECT
            PULocationID AS zone_id,
            window_end,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS revenue
        FROM TABLE(
            TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
        )
        GROUP BY PULocationID, window_end
    """)

    # Output 2: Top zones (stateful - cumulative)
    stmt_set.add_insert_sql("""
        INSERT INTO dashboard_top_zones
        SELECT
            PULocationID AS zone_id,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue
        FROM rides
        GROUP BY PULocationID
    """)

    # Output 3: Average stats (stateful)
    stmt_set.add_insert_sql("""
        INSERT INTO dashboard_avg_stats
        SELECT
            PULocationID AS zone_id,
            AVG(total_amount) AS avg_fare,
            AVG(trip_distance) AS avg_distance,
            COUNT(*) AS trip_count
        FROM rides
        GROUP BY PULocationID
    """)

    # Output 4: Total event count (monitoring)
    stmt_set.add_insert_sql("""
        INSERT INTO dashboard_processing_stats
        SELECT
            'total_events' AS stat_name,
            COUNT(*) AS stat_value,
            MAX(event_time) AS last_updated
        FROM rides
    """)

    stmt_set.execute().wait()

if __name__ == "__main__":
    main()
