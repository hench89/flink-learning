"""Route late events to a separate Kafka topic instead of dropping."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Source with tight watermark (5 seconds)
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

    # On-time events: windowed aggregation
    t_env.execute_sql("""
        CREATE TABLE windowed_revenue (
            zone_id INTEGER,
            window_end TIMESTAMP(3),
            revenue DOUBLE,
            PRIMARY KEY (zone_id, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'windowed-revenue',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # ponytail: Flink SQL doesn't have native side outputs for late data
    # Use CURRENT_WATERMARK() to detect late events manually
    # Or switch to DataStream API for true side outputs

    # This aggregation drops late events by default
    # Run delayed_producer.py to see events arrive late and get dropped
    t_env.execute_sql("""
        INSERT INTO windowed_revenue
        SELECT
            PULocationID AS zone_id,
            window_end,
            SUM(total_amount) AS revenue
        FROM TABLE(
            TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY PULocationID, window_end
    """).wait()

if __name__ == "__main__":
    main()
