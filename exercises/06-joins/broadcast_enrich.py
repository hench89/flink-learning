"""Broadcast State: Enrich stream with small reference data.

Use broadcast when dimension data is small enough to fit on every worker.
Updates propagate to all parallel instances — no shuffle needed.

Pattern: Broadcast reference data, join in memory on each worker.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import BroadcastProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
import json


# Descriptor for broadcast state — same instance used everywhere
ZONE_STATE = MapStateDescriptor("zones", Types.INT(), Types.STRING())


class ZoneEnrichment(BroadcastProcessFunction):
    """Enrich rides with zone names from broadcast state."""

    def process_element(self, ride, ctx):
        """Process ride against broadcast zone data."""
        zones = ctx.get_broadcast_state(ZONE_STATE)

        pu_zone = zones.get(ride.get("PULocationID"))
        do_zone = zones.get(ride.get("DOLocationID"))

        yield {
            **ride,
            "pickup_zone": pu_zone or "Unknown",
            "dropoff_zone": do_zone or "Unknown",
        }

    def process_broadcast_element(self, zone, ctx):
        """Update broadcast state when zone data changes."""
        zones = ctx.get_broadcast_state(ZONE_STATE)
        zones.put(zone["zone_id"], zone["zone_name"])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)

    # Main stream: rides (high volume)
    rides_source = KafkaSource.builder() \
        .set_bootstrap_servers("redpanda:29092") \
        .set_topics("rides") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Broadcast stream: zone reference data (low volume, small)
    zones_source = KafkaSource.builder() \
        .set_bootstrap_servers("redpanda:29092") \
        .set_topics("zones") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    rides = env.from_source(rides_source, "rides") \
        .map(lambda x: json.loads(x))

    zones = env.from_source(zones_source, "zones") \
        .map(lambda x: json.loads(x)) \
        .broadcast(ZONE_STATE)

    # Connect main stream with broadcast stream
    enriched = rides.connect(zones) \
        .process(ZoneEnrichment()) \
        .map(lambda x: json.dumps(x))

    enriched.print()
    env.execute("Broadcast Enrichment")


if __name__ == "__main__":
    main()
