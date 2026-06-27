"""Stream-table join: Enrich rides with zone names from PostgreSQL."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Streaming source: Kafka rides
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

    # Lookup table: PostgreSQL zones (static/slowly changing)
    t_env.execute_sql("""
        CREATE TABLE taxi_zones (
            zone_id INTEGER,
            borough STRING,
            zone_name STRING,
            PRIMARY KEY (zone_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'taxi_zones',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver',
            'lookup.cache.max-rows' = '1000',
            'lookup.cache.ttl' = '1h'
        )
    """)

    # Sink: enriched rides
    t_env.execute_sql("""
        CREATE TABLE enriched_rides (
            pickup_zone_id INTEGER,
            pickup_zone_name STRING,
            dropoff_zone_id INTEGER,
            dropoff_zone_name STRING,
            fare DOUBLE,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'enriched-rides',
            'format' = 'json'
        )
    """)

    # Temporal join: lookup zone names for both pickup and dropoff
    t_env.execute_sql("""
        INSERT INTO enriched_rides
        SELECT
            r.PULocationID AS pickup_zone_id,
            COALESCE(pz.zone_name, 'Unknown') AS pickup_zone_name,
            r.DOLocationID AS dropoff_zone_id,
            COALESCE(dz.zone_name, 'Unknown') AS dropoff_zone_name,
            r.total_amount AS fare,
            r.event_time
        FROM rides r
        LEFT JOIN taxi_zones FOR SYSTEM_TIME AS OF r.event_time AS pz
            ON r.PULocationID = pz.zone_id
        LEFT JOIN taxi_zones FOR SYSTEM_TIME AS OF r.event_time AS dz
            ON r.DOLocationID = dz.zone_id
    """).wait()

if __name__ == "__main__":
    main()
