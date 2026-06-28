"""Rate limiting: Alert when a zone exceeds N rides per minute."""

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
        CREATE TABLE zone_rate_alerts (
            zone_id INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            ride_count BIGINT,
            PRIMARY KEY (zone_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'zone_rate_alerts',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """)

    # ponytail: rate limit = tumbling window + HAVING filter
    # Only emit when count exceeds threshold (20 rides/minute = busy zone)
    t_env.execute_sql("""
        INSERT INTO zone_rate_alerts
        SELECT
            zone_id,
            window_start,
            window_end,
            ride_count
        FROM (
            SELECT
                PULocationID AS zone_id,
                window_start,
                window_end,
                COUNT(*) AS ride_count
            FROM TABLE(
                TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
            )
            GROUP BY PULocationID, window_start, window_end
        )
        WHERE ride_count > 20
    """)

if __name__ == "__main__":
    main()
