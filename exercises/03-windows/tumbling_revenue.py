"""Tumbling window: Hourly revenue per zone."""

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

    t_env.execute_sql("""
        CREATE TABLE minute_revenue (
            zone_id INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            total_revenue DOUBLE,
            PRIMARY KEY (zone_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'minute_revenue',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO minute_revenue
        SELECT
            PULocationID AS zone_id,
            window_start,
            window_end,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY PULocationID, window_start, window_end
    """)
if __name__ == "__main__":
    main()
