"""Side-by-side: Cumulative state vs hourly window counter."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

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

    # Sink 1: Stateful cumulative totals (never resets)
    t_env.execute_sql("""
        CREATE TABLE zone_cumulative (
            zone_id INTEGER,
            total_trips BIGINT,
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'zone_cumulative',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Sink 2: Windowed hourly counts (resets each hour)
    t_env.execute_sql("""
        CREATE TABLE zone_hourly (
            zone_id INTEGER,
            window_end TIMESTAMP(3),
            hourly_trips BIGINT,
            PRIMARY KEY (zone_id, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'zone_hourly',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Create statement set for multiple inserts from same source
    stmt_set = t_env.create_statement_set()

    # State: cumulative (no window)
    stmt_set.add_insert_sql("""
        INSERT INTO zone_cumulative
        SELECT PULocationID AS zone_id, COUNT(*) AS total_trips
        FROM rides
        GROUP BY PULocationID
    """)

    # Window: hourly (resets)
    stmt_set.add_insert_sql("""
        INSERT INTO zone_hourly
        SELECT
            PULocationID AS zone_id,
            window_end,
            COUNT(*) AS hourly_trips
        FROM TABLE(
            TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
        )
        GROUP BY PULocationID, window_end
    """)

    stmt_set.execute().wait()

if __name__ == "__main__":
    main()
