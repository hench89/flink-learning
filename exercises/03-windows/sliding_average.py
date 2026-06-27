"""Sliding window: 5-minute rolling average fare, sliding every 1 minute."""

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
        CREATE TABLE rolling_avg_fare (
            zone_id INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            avg_fare DOUBLE,
            PRIMARY KEY (zone_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'rolling_avg_fare',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Sliding: 5-minute window, slides every 1 minute
    # Compare row count with tumbling—sliding produces more rows
    t_env.execute_sql("""
        INSERT INTO rolling_avg_fare
        SELECT
            PULocationID AS zone_id,
            window_start,
            window_end,
            COUNT(*) AS trip_count,
            AVG(total_amount) AS avg_fare
        FROM TABLE(
            HOP(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
        )
        GROUP BY PULocationID, window_start, window_end
    """).wait()

if __name__ == "__main__":
    main()
