"""Filter: Only emit rides where fare > $20."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Source: Kafka taxi rides
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

    # Sink: Kafka topic for high-fare rides
    t_env.execute_sql("""
        CREATE TABLE high_fare_rides (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'high-fare-rides',
            'format' = 'json'
        )
    """)

    # Filter: total_amount > 20
    t_env.execute_sql("""
        INSERT INTO high_fare_rides
        SELECT PULocationID, DOLocationID, trip_distance, total_amount
        FROM rides
        WHERE total_amount > 20
    """).wait()

if __name__ == "__main__":
    main()
