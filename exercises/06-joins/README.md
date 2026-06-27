# 06 - Joins: Combining Streams

**Goal:** Enrich and correlate streams.

## Concept

| Join Type | Use Case |
|-----------|----------|
| **Stream-Table** | Enrich events with static lookup data |
| **Stream-Stream** | Correlate two event streams by key |
| **Interval Join** | Match events within a time window |

## Setup

Create the zones lookup table in PostgreSQL:

```bash
make db-connect
```

```sql
\i exercises/06-joins/setup_zones_table.sql
\q
```

**File:** [setup_zones_table.sql](setup_zones_table.sql)

## Exercises

### 1. Stream-Table Join: Enrich with Zone Names

Join taxi rides with zone names from PostgreSQL.

```bash
make submit-job JOB=exercises/06-joins/stream_table_join.py
make kafka-consume TOPIC=enriched-rides LINES=5
```

Output includes `pickup_zone_name` and `dropoff_zone_name`.

**Solution:** [stream_table_join.py](stream_table_join.py)

### 2. Stream-Stream Join: Pickup + Dropoff

Correlate pickup and dropoff events by `trip_id` within 30 minutes.

First, run a producer that emits separate pickup/dropoff events:
```bash
uv run exercises/06-joins/split_producer.py
```

Then join them:
```bash
make submit-job JOB=exercises/06-joins/stream_stream_join.py
make kafka-consume TOPIC=completed-trips LINES=5
```

Output shows trip duration (dropoff_time - pickup_time).

**Solution:** [stream_stream_join.py](stream_stream_join.py)

### 3. Interval Join: Nearby Rides

Find rides starting within 5 minutes of each other at the same zone.

```bash
make submit-job JOB=exercises/06-joins/interval_join.py
make db-query SQL="SELECT * FROM nearby_rides LIMIT 10;"
```

**Solution:** [interval_join.py](interval_join.py)

## Critical Teaching: Joins Fail Silently

Add metrics to see joins working:

```python
# Count matched, unmatched, expired
matched_counter = getRuntimeContext().getMetricGroup().counter("matched")
unmatched_counter = getRuntimeContext().getMetricGroup().counter("unmatched")
```

Check in Flink UI: Jobs → Your Job → Metrics

## Realistic Use Case

**Fraud detection:** Pickup + dropoff join with 30-minute window. Flag rides where:
- `trip_duration > 2 hours` (suspicious)
- `dropoff_zone == pickup_zone` and `fare > $50` (circular trip)

## Verify

```bash
# Enriched rides have zone names
make kafka-consume TOPIC=enriched-rides LINES=3

# Completed trips have duration
make kafka-consume TOPIC=completed-trips LINES=3

# Check join metrics in Flink UI
make ui
```

## Next

Proceed to [07-project](../07-project/) to build a capstone dashboard combining all concepts.
