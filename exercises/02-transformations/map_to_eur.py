"""Map: Convert fare to EUR and rename fields."""

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
            total_amount DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'rides',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE rides_eur (
            pickup_zone INTEGER,
            dropoff_zone INTEGER,
            distance_miles DOUBLE,
            fare_eur DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'rides-eur',
            'format' = 'json'
        )
    """)

    # Map: rename fields, convert USD to EUR
    t_env.execute_sql("""
        INSERT INTO rides_eur
        SELECT
            PULocationID AS pickup_zone,
            DOLocationID AS dropoff_zone,
            trip_distance AS distance_miles,
            total_amount * 0.92 AS fare_eur
        FROM rides
    """)

if __name__ == "__main__":
    main()
