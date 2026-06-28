"""Produce zone reference data for broadcast_enrich exercise."""

import json
from kafka import KafkaProducer

# NYC taxi zone subset (matches taxi data PULocationID/DOLocationID)
ZONES = [
    {"zone_id": 1, "zone_name": "Newark Airport"},
    {"zone_id": 4, "zone_name": "Alphabet City"},
    {"zone_id": 7, "zone_name": "Astoria"},
    {"zone_id": 12, "zone_name": "Battery Park"},
    {"zone_id": 13, "zone_name": "Battery Park City"},
    {"zone_id": 24, "zone_name": "Bloomingdale"},
    {"zone_id": 41, "zone_name": "Central Harlem"},
    {"zone_id": 42, "zone_name": "Central Harlem North"},
    {"zone_id": 43, "zone_name": "Central Park"},
    {"zone_id": 48, "zone_name": "Clinton East"},
    {"zone_id": 50, "zone_name": "Clinton West"},
    {"zone_id": 68, "zone_name": "East Chelsea"},
    {"zone_id": 79, "zone_name": "East Village"},
    {"zone_id": 87, "zone_name": "Financial District North"},
    {"zone_id": 88, "zone_name": "Financial District South"},
    {"zone_id": 90, "zone_name": "Flatiron"},
    {"zone_id": 100, "zone_name": "Garment District"},
    {"zone_id": 107, "zone_name": "Gramercy"},
    {"zone_id": 113, "zone_name": "Greenwich Village North"},
    {"zone_id": 114, "zone_name": "Greenwich Village South"},
    {"zone_id": 125, "zone_name": "Hudson Sq"},
    {"zone_id": 137, "zone_name": "Kips Bay"},
    {"zone_id": 140, "zone_name": "Lenox Hill East"},
    {"zone_id": 141, "zone_name": "Lenox Hill West"},
    {"zone_id": 142, "zone_name": "Lincoln Square East"},
    {"zone_id": 143, "zone_name": "Lincoln Square West"},
    {"zone_id": 144, "zone_name": "Little Italy/NoLiTa"},
    {"zone_id": 148, "zone_name": "Lower East Side"},
    {"zone_id": 151, "zone_name": "Manhattan Valley"},
    {"zone_id": 152, "zone_name": "Manhattanville"},
    {"zone_id": 153, "zone_name": "Marble Hill"},
    {"zone_id": 158, "zone_name": "Meatpacking/West Village West"},
    {"zone_id": 161, "zone_name": "Midtown Center"},
    {"zone_id": 162, "zone_name": "Midtown East"},
    {"zone_id": 163, "zone_name": "Midtown North"},
    {"zone_id": 164, "zone_name": "Midtown South"},
    {"zone_id": 166, "zone_name": "Morningside Heights"},
    {"zone_id": 170, "zone_name": "Murray Hill"},
    {"zone_id": 186, "zone_name": "Penn Station/Madison Sq West"},
    {"zone_id": 209, "zone_name": "Seaport"},
    {"zone_id": 211, "zone_name": "SoHo"},
    {"zone_id": 224, "zone_name": "Stuy Town/PCV"},
    {"zone_id": 229, "zone_name": "Sutton Place/Turtle Bay North"},
    {"zone_id": 230, "zone_name": "Sutton Place/Turtle Bay South"},
    {"zone_id": 231, "zone_name": "Times Sq/Theatre District"},
    {"zone_id": 232, "zone_name": "TriBeCa/Civic Center"},
    {"zone_id": 233, "zone_name": "Two Bridges/Seward Park"},
    {"zone_id": 234, "zone_name": "Union Sq"},
    {"zone_id": 236, "zone_name": "Upper East Side North"},
    {"zone_id": 237, "zone_name": "Upper East Side South"},
    {"zone_id": 238, "zone_name": "Upper West Side North"},
    {"zone_id": 239, "zone_name": "Upper West Side South"},
    {"zone_id": 243, "zone_name": "Washington Heights North"},
    {"zone_id": 244, "zone_name": "Washington Heights South"},
    {"zone_id": 246, "zone_name": "West Chelsea/Hudson Yards"},
    {"zone_id": 249, "zone_name": "West Village"},
    {"zone_id": 261, "zone_name": "World Trade Center"},
    {"zone_id": 262, "zone_name": "Yorkville East"},
    {"zone_id": 263, "zone_name": "Yorkville West"},
]

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Publishing {len(ZONES)} zones to 'zones' topic...")
    for zone in ZONES:
        producer.send("zones", value=zone)
        print(f"  {zone['zone_id']}: {zone['zone_name']}")

    producer.flush()
    print("Done. Zone data is now available for broadcast enrichment.")

if __name__ == "__main__":
    main()
