"""Taxi ride producer - generates NYC taxi ride events to Kafka."""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rides"

# NYC taxi zones (subset)
ZONES = [
    161, 162, 163, 164, 166, 170, 186, 209, 211, 224,
    229, 230, 231, 232, 233, 234, 236, 237, 238, 239,
    243, 244, 246, 249, 261, 262, 263, 264, 265
]

def generate_ride():
    """Generate a random taxi ride event."""
    now = datetime.now()
    pickup_zone = random.choice(ZONES)
    dropoff_zone = random.choice(ZONES)

    return {
        "PULocationID": pickup_zone,
        "DOLocationID": dropoff_zone,
        "trip_distance": round(random.uniform(0.5, 25.0), 2),
        "total_amount": round(random.uniform(5.0, 150.0), 2),
        "tpep_pickup_datetime": int(now.timestamp() * 1000),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing taxi rides to {TOPIC}... (Ctrl+C to stop)")
    print(f"Events per second: 10\n")

    try:
        count = 0
        while True:
            ride = generate_ride()
            producer.send(TOPIC, ride)
            count += 1

            if count % 10 == 0:
                print(f"[{count}] zone {ride['PULocationID']} → {ride['DOLocationID']} "
                      f"${ride['total_amount']:.2f}")

            time.sleep(0.1)  # 10 events/sec

    except KeyboardInterrupt:
        print(f"\nStopped. Produced {count} events.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
