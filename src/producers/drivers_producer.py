"""Produce driver status updates for coprocess_join exercise."""

import json
import time
import random
from kafka import KafkaProducer

DRIVERS = [
    {"driver_id": 1001, "name": "Alice Chen", "rating": 4.9},
    {"driver_id": 1002, "name": "Bob Smith", "rating": 4.7},
    {"driver_id": 1003, "name": "Carol Jones", "rating": 4.8},
    {"driver_id": 1004, "name": "David Lee", "rating": 4.6},
    {"driver_id": 1005, "name": "Eva Garcia", "rating": 4.95},
]

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Publishing driver updates to 'drivers' topic...")
    for driver in DRIVERS:
        producer.send("drivers", value=driver)
        print(f"  Sent: {driver['name']} (id={driver['driver_id']})")
        time.sleep(0.5)

    # Periodically update ratings
    print("\nSending periodic rating updates (Ctrl+C to stop)...")
    while True:
        driver = random.choice(DRIVERS).copy()
        driver["rating"] = round(random.uniform(4.5, 5.0), 2)
        producer.send("drivers", value=driver)
        print(f"  Updated: {driver['name']} rating={driver['rating']}")
        time.sleep(5)

if __name__ == "__main__":
    main()
