"""Observe watermark progress and late events."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Source with 5-second watermark lag
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

    # Print sink to see events with their timestamps
    # Check taskmanager logs: make logs SVC=taskmanager
    t_env.execute_sql("""
        CREATE TABLE event_log (
            zone_id INTEGER,
            fare DOUBLE,
            event_time TIMESTAMP(3),
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'print'
        )
    """)

    # ponytail: 'print' connector dumps to taskmanager stdout
    # Watch with: make logs SVC=taskmanager
    t_env.execute_sql("""
        INSERT INTO event_log
        SELECT
            PULocationID AS zone_id,
            total_amount AS fare,
            event_time
        FROM rides
    """)

if __name__ == "__main__":
    main()
