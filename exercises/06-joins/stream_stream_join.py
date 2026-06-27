"""Stream-stream join: Correlate pickup and dropoff events by trip_id."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Stream 1: Pickup events
    t_env.execute_sql("""
        CREATE TABLE pickups (
            trip_id STRING,
            pickup_zone INTEGER,
            pickup_time TIMESTAMP(3),
            WATERMARK FOR pickup_time AS pickup_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'taxi-pickups',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Stream 2: Dropoff events
    t_env.execute_sql("""
        CREATE TABLE dropoffs (
            trip_id STRING,
            dropoff_zone INTEGER,
            dropoff_time TIMESTAMP(3),
            fare DOUBLE,
            WATERMARK FOR dropoff_time AS dropoff_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'taxi-dropoffs',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Sink: completed trips
    t_env.execute_sql("""
        CREATE TABLE completed_trips (
            trip_id STRING,
            pickup_zone INTEGER,
            dropoff_zone INTEGER,
            pickup_time TIMESTAMP(3),
            dropoff_time TIMESTAMP(3),
            duration_minutes BIGINT,
            fare DOUBLE,
            PRIMARY KEY (trip_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'completed-trips',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # Interval join: match pickup + dropoff within 30 minutes
    # ponytail: this requires split_producer.py to emit separate pickup/dropoff events
    t_env.execute_sql("""
        INSERT INTO completed_trips
        SELECT
            p.trip_id,
            p.pickup_zone,
            d.dropoff_zone,
            p.pickup_time,
            d.dropoff_time,
            TIMESTAMPDIFF(MINUTE, p.pickup_time, d.dropoff_time) AS duration_minutes,
            d.fare
        FROM pickups p
        JOIN dropoffs d
            ON p.trip_id = d.trip_id
            AND d.dropoff_time BETWEEN p.pickup_time AND p.pickup_time + INTERVAL '30' MINUTE
    """).wait()

if __name__ == "__main__":
    main()
