"""ValueState: Cumulative trip counter per zone (never resets)."""

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

    # Upsert sink: state updates overwrite previous values
    t_env.execute_sql("""
        CREATE TABLE zone_trip_totals (
            zone_id INTEGER,
            total_trips BIGINT,
            total_revenue DOUBLE,
            last_updated TIMESTAMP(3),
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'zone-totals',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # ponytail: GROUP BY without window = unbounded aggregation (stateful)
    # This accumulates forever—use TTL in production
    t_env.execute_sql("""
        INSERT INTO zone_trip_totals
        SELECT
            PULocationID AS zone_id,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            MAX(event_time) AS last_updated
        FROM rides
        GROUP BY PULocationID
    """)

if __name__ == "__main__":
    main()
