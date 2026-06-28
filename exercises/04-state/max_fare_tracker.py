"""ReducingState: Track max fare ever seen per zone."""

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
        CREATE TABLE zone_max_fare (
            zone_id INTEGER,
            max_fare DOUBLE,
            max_distance DOUBLE,
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'zone-max-fare',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # MAX() is stateful: Flink remembers highest value per key
    t_env.execute_sql("""
        INSERT INTO zone_max_fare
        SELECT
            PULocationID AS zone_id,
            MAX(total_amount) AS max_fare,
            MAX(trip_distance) AS max_distance
        FROM rides
        GROUP BY PULocationID
    """)

if __name__ == "__main__":
    main()
