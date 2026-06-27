"""Producer that intentionally delays some events to simulate out-of-order arrival."""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rides"

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    zones = [161, 162, 163, 164, 165, 166, 167, 168, 169, 170]

    print("Sending delayed events... (Ctrl+C to stop)")
    print("Some events will have timestamps 5-30 minutes in the past.\n")

    try:
        while True:
            now = datetime.now()

            # 30% of events are delayed (simulating late arrival)
            if random.random() < 0.3:
                delay_minutes = random.randint(5, 30)
                event_time = now - timedelta(minutes=delay_minutes)
                status = f"LATE by {delay_minutes}min"
            else:
                event_time = now
                status = "on-time"

            ride = {
                "PULocationID": random.choice(zones),
                "DOLocationID": random.choice(zones),
                "trip_distance": round(random.uniform(0.5, 20.0), 2),
                "total_amount": round(random.uniform(5.0, 100.0), 2),
                "tpep_pickup_datetime": int(event_time.timestamp() * 1000),
            }

            producer.send(TOPIC, ride)

            print(f"[{status:15}] event_time={event_time.strftime('%H:%M:%S')} "
                  f"zone={ride['PULocationID']} fare=${ride['total_amount']:.2f}")

            time.sleep(0.1)  # 10 events/sec

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
