"""Interval join: Find rides starting within 5 minutes of each other at the same zone."""

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
        CREATE TABLE nearby_rides (
            zone_id INTEGER,
            ride1_time TIMESTAMP(3),
            ride1_fare DOUBLE,
            ride2_time TIMESTAMP(3),
            ride2_fare DOUBLE,
            time_diff_seconds BIGINT,
            PRIMARY KEY (zone_id, ride1_time, ride2_time) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'nearby_rides',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Self-join: find pairs of rides from same zone within 5 minutes
    # ponytail: self-joins are expensive on state—use for analysis, not production
    t_env.execute_sql("""
        INSERT INTO nearby_rides
        SELECT
            r1.PULocationID AS zone_id,
            r1.event_time AS ride1_time,
            r1.total_amount AS ride1_fare,
            r2.event_time AS ride2_time,
            r2.total_amount AS ride2_fare,
            TIMESTAMPDIFF(SECOND, r1.event_time, r2.event_time) AS time_diff_seconds
        FROM rides r1
        JOIN rides r2
            ON r1.PULocationID = r2.PULocationID
            AND r2.event_time BETWEEN r1.event_time AND r1.event_time + INTERVAL '5' MINUTE
            AND r1.event_time < r2.event_time
    """).wait()

if __name__ == "__main__":
    main()
