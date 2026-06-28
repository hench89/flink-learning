# 04 - State: Keyed State Management

**Goal:** Learn when and how to use state instead of windows.

## Concept

**Windows reset. State persists.**

### Window vs State

```sql
-- WINDOW: wrap table in TUMBLE/HOP/SESSION → state resets when window closes
SELECT zone_id, COUNT(*) FROM TABLE(
    TUMBLE(TABLE rides, DESCRIPTOR(event_time), INTERVAL '1' HOUR)
) GROUP BY zone_id, window_start, window_end

-- STATE: no wrapper → state accumulates forever
SELECT zone_id, COUNT(*) FROM rides GROUP BY zone_id
```

### SQL Pattern → Flink State

Without `TUMBLE`/`HOP`/`SESSION`, Flink keeps state per key:

| SQL Pattern | Underlying State | What's Stored |
|-------------|------------------|---------------|
| `COUNT(*)` | ValueState | Running count |
| `SUM(amount)` | ValueState | Running total |
| `MAX(fare)` | ReducingState | Highest value seen |
| `MIN(fare)` | ReducingState | Lowest value seen |
| `AVG(fare)` | ValueState (2 values) | Sum + count |

### Other Stateful Patterns

| Pattern | Use Case | State Type |
|---------|----------|------------|
| **Deduplication** | Ignore duplicate events | ValueState (seen flag) |
| **Last N items** | Recent history per key | ListState |
| **Running average** | Smoothed metrics | ValueState (sum + count) |
| **First seen** | Track when key first appeared | ValueState (timestamp) |
| **Change detection** | Alert when value differs from previous | ValueState (last value) |
| **Rate limiting** | Count events per time period | MapState (time → count) |

## Setup

```bash
make db-setup FILE=exercises/04-state/setup_tables.sql
```

## Exercises

### 1. Cumulative Trip Counter

Count total trips per zone — **never resets**, even across restarts.

```bash
make submit-job JOB=exercises/04-state/cumulative_counter.py
```

Wait 30 seconds, then check:
```bash
make db-query SQL="SELECT * FROM zone_trip_totals ORDER BY total_trips DESC LIMIT 5;"
```

Run the query again a minute later. **Totals keep growing** — that's state, not windows.

**Solution:** [cumulative_counter.py](cumulative_counter.py) — `GROUP BY` without a window = unbounded aggregation

---

### 2. Max Fare Tracker

Track the **highest fare ever seen** per zone.

```bash
make submit-job JOB=exercises/04-state/max_fare_tracker.py
```

Check results:
```bash
make db-query SQL="SELECT * FROM zone_max_fare ORDER BY max_fare DESC LIMIT 5;"
```

The max can only go up. Even if all new rides are cheap, the stored max stays.

**Solution:** [max_fare_tracker.py](max_fare_tracker.py) — `MAX()` remembers the highest value per key

---

### 3. Rate Limiter

Alert when a zone exceeds 20 rides per minute — a common fraud/abuse detection pattern.

```bash
make submit-job JOB=exercises/04-state/rate_limiter.py
```

Check for alerts:
```bash
make db-query SQL="SELECT * FROM zone_rate_alerts ORDER BY window_end DESC LIMIT 10;"
```

**What you'll see:** Only busy zones appear. Quiet zones never show up.

This is the pattern behind:
- API rate limiting (>100 requests/min → throttle)
- Fraud detection (>10 transactions/min → flag)
- Alerting (>50 errors/min → page oncall)

**Solution:** [rate_limiter.py](rate_limiter.py) — `TUMBLE` + `HAVING` filter

## Checkpointing

State survives job restarts via checkpoints. Test it:

1. Note current totals: `make db-query SQL="SELECT * FROM zone_trip_totals LIMIT 3;"`
2. Kill the job: `make job-status` then `make kill-job JOB_ID=<id>`
3. Restart: `make submit-job JOB=exercises/04-state/cumulative_counter.py`
4. Verify totals continue from where they left off

## Pitfalls

**State size explosion:**
```python
# Bad: stores full history
state.add(ride)  # ListState grows forever

# Good: store only what you need
state.update(state.value() + 1)  # ValueState stays small
```

**TTL for cleanup:**
```python
# State expires after 1 hour of no updates
state_descriptor.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(1)).build())
```

## Verify

Kill and restart the cumulative counter job. State should survive.

## Next

Proceed to [05-watermarks](../05-watermarks/) to handle late-arriving data.
