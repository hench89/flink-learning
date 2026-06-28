"""KeyedCoProcessFunction: Custom stateful join with buffering.

Unlike SQL joins, you control exactly when to emit and what state to keep.
Production enrichment pipelines chain multiple coprocess joins to combine
10+ streams with custom buffering and emit logic.

Pattern: Buffer one stream, emit when the other arrives.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedCoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
import json


class RideDriverJoin(KeyedCoProcessFunction):
    """Join rides with driver status. Buffer rides until driver info arrives."""

    def open(self, ctx: RuntimeContext):
        # State per driver_id (the key)
        self.driver_state = ctx.get_state(
            ValueStateDescriptor("driver", Types.STRING())
        )
        self.pending_rides = ctx.get_list_state(
            ValueStateDescriptor("pending_rides", Types.STRING())
        )

    def process_element1(self, ride, ctx):
        """Process ride event. Emit if driver known, else buffer."""
        driver_info = self.driver_state.value()
        if driver_info:
            # Driver known — emit enriched ride immediately
            yield self._enrich(ride, json.loads(driver_info))
        else:
            # Buffer ride until driver info arrives
            self.pending_rides.add(json.dumps(ride))

    def process_element2(self, driver, ctx):
        """Process driver event. Update state, flush buffered rides."""
        self.driver_state.update(json.dumps(driver))

        # Emit all buffered rides with new driver info
        for ride_json in self.pending_rides.get():
            ride = json.loads(ride_json)
            yield self._enrich(ride, driver)
        self.pending_rides.clear()

    def _enrich(self, ride, driver):
        return {
            **ride,
            "driver_name": driver.get("name"),
            "driver_rating": driver.get("rating"),
        }


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)

    # Stream 1: Rides (keyed by driver_id)
    rides_source = KafkaSource.builder() \
        .set_bootstrap_servers("redpanda:29092") \
        .set_topics("rides") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Stream 2: Driver updates (keyed by driver_id)
    drivers_source = KafkaSource.builder() \
        .set_bootstrap_servers("redpanda:29092") \
        .set_topics("drivers") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    rides = env.from_source(rides_source, "rides") \
        .map(lambda x: json.loads(x)) \
        .key_by(lambda r: r.get("driver_id", 0))

    drivers = env.from_source(drivers_source, "drivers") \
        .map(lambda x: json.loads(x)) \
        .key_by(lambda d: d["driver_id"])

    # Connect and apply custom join logic
    enriched = rides.connect(drivers) \
        .process(RideDriverJoin()) \
        .map(lambda x: json.dumps(x))

    enriched.print()
    env.execute("CoProcess Join")


if __name__ == "__main__":
    main()
