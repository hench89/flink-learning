"""Session window: Group trips by driver until 30-minute inactivity gap."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # ponytail: workshop data doesn't have driver_id, using DOLocationID as proxy
    # Real exercise: add driver_id to producer or use VendorID
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
        CREATE TABLE driver_sessions (
            dropoff_zone INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            session_revenue DOUBLE,
            PRIMARY KEY (dropoff_zone, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'driver_sessions',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Session window: groups trips until 30-min gap
    # Variable-sized windows based on activity patterns
    t_env.execute_sql("""
        INSERT INTO driver_sessions
        SELECT
            DOLocationID AS dropoff_zone,
            window_start AS session_start,
            window_end AS session_end,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS session_revenue
        FROM TABLE(
            SESSION(TABLE rides PARTITION BY DOLocationID, DESCRIPTOR(event_time), INTERVAL '30' MINUTE)
        )
        GROUP BY DOLocationID, window_start, window_end
    """).wait()

if __name__ == "__main__":
    main()
