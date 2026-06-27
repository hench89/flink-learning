"""FlatMap: Emit separate pickup and dropoff events per ride."""

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
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE zone_events (
            event_type STRING,
            zone_id INTEGER,
            event_timestamp TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'zone-events',
            'format' = 'json'
        )
    """)

    # FlatMap via UNION: emit pickup event + dropoff event per ride
    # ponytail: SQL UNION is the lazy flatmap—no UDF needed
    t_env.execute_sql("""
        INSERT INTO zone_events
        SELECT 'pickup' AS event_type, PULocationID AS zone_id, event_time AS event_timestamp
        FROM rides
        UNION ALL
        SELECT 'dropoff' AS event_type, DOLocationID AS zone_id, event_time AS event_timestamp
        FROM rides
    """).wait()

if __name__ == "__main__":
    main()
