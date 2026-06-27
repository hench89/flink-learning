# 02 - Transformations: Stateless Operations

**Goal:** Learn map, filter, flatMap—operations that process each record independently.

## Background

Before starting, read [Tables = Kafka Topics](../../docs/concepts.md#tables--kafka-topics)—you'll see `CREATE TABLE` in the code and should understand how it maps to Kafka.

## Concept

Stateless transformations don't remember anything between records:

| Operation | Input → Output | Use Case |
|-----------|----------------|----------|
| **filter** | 1 → 0 or 1 | Discard records not matching a condition |
| **map** | 1 → 1 | Transform each record to a new structure |
| **flatMap** | 1 → 0, 1, or many | Split one record into multiple |

---

## Exercises

### 0. Job Management (Warm-up)

Before diving into transformations, practice the job lifecycle:

```bash
# Submit the filter job
make submit-job JOB=exercises/02-transformations/filter_rides.py

# List running jobs (note the JOB_ID)
make job-status

# Or view in Flink UI
make ui
```

Now kill it:
```bash
make kill-job JOB_ID=<paste-id-here>

# Or click "Cancel Job" in the Flink UI
```

> **Why this matters:** You'll submit and kill jobs many times while learning. Get comfortable with this before modifying code.

---

### 1. Filter: High-Fare Rides

Only emit rides where `total_amount > 20`. The output stream will be smaller than the input.

```bash
make submit-job JOB=exercises/02-transformations/filter_rides.py
make consumer-follow TOPIC=high-fare-rides
```

> **What's happening:** Flink reads every ride from `rides` topic, checks if `total_amount > 20`, and only forwards matching records to `high-fare-rides`.

**Solution:** [filter_rides.py](filter_rides.py)

---

### 2. Map: Convert to EUR

Transform each record 1:1 with a new structure:
- Convert `total_amount` USD → EUR (×0.92)
- Rename fields to cleaner names

```bash
make submit-job JOB=exercises/02-transformations/map_to_eur.py
make consumer-follow TOPIC=rides-eur
```

> **What's happening:** Every input record produces exactly one output record. The rate stays the same, only the structure changes.

**Solution:** [map_to_eur.py](map_to_eur.py)

---

### 3. FlatMap: Split Events

Emit TWO events per ride:
- `{"event_type": "pickup", "zone_id": 161, ...}`
- `{"event_type": "dropoff", "zone_id": 237, ...}`

```bash
make submit-job JOB=exercises/02-transformations/flatmap_events.py
make consumer-follow TOPIC=zone-events
```

> **What's happening:** Each input ride produces 2 output events. The output rate is 2× the input. This decomposition is useful for counting entries/exits per zone (you'll do that in module 03).

**Solution:** [flatmap_events.py](flatmap_events.py)

---

## Verify

Run all three jobs (they can run simultaneously), then check each output topic:

```bash
make consumer-follow TOPIC=high-fare-rides
make consumer-follow TOPIC=rides-eur
make consumer-follow TOPIC=zone-events
```

## Try This (Optional)

Practice modifying and resubmitting jobs. Remember to kill the old job first!

1. **Filter:** Change to `total_amount > 50 AND trip_distance > 5` to find premium long-distance rides
2. **Map:** Add a computed field `price_per_mile` (fare_eur / distance_miles)
3. **FlatMap:** Add a third event type `ride_complete` that includes the `total_amount`

## Next

Proceed to [03-windows](../03-windows/) to aggregate records over time.
