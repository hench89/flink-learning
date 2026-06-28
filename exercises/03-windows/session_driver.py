"""Session window: Group events until inactivity gap.

NOTE: Session windows are tricky to demo because they need gaps in the data.
With our continuous producer, sessions rarely close. This example uses a
10-second gap so you can see results by pausing the producer briefly.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.execute_sql("""
        CREATE TABLE rides (
            driver_id INTEGER,
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
        CREATE TABLE driver_sessions (
            driver_id INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            trip_count BIGINT,
            total_revenue DOUBLE,
            PRIMARY KEY (driver_id, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'driver_sessions',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Session window: groups events until 10-second gap per driver
    # To see sessions close: stop the producer for 15+ seconds, then restart
    t_env.execute_sql("""
        INSERT INTO driver_sessions
        SELECT
            driver_id,
            window_start AS session_start,
            window_end AS session_end,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            SESSION(TABLE rides PARTITION BY driver_id, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
        )
        GROUP BY driver_id, window_start, window_end
    """)
if __name__ == "__main__":
    main()
