# 04 - State: Keyed State Management

**Goal:** Learn when and how to use state instead of windows.

## Concept

**Windows reset. State persists.**

Use state when you need:
- Cumulative totals (all-time counts)
- Running statistics (max ever seen)
- Cross-window memory

| State Type | Use Case |
|------------|----------|
| **ValueState** | Single value per key (counter, sum) |
| **ReducingState** | Aggregate with reduce function (max, min) |
| **ListState** | Collection per key (history, buffer) |

## Exercises

### 1. ValueState: Cumulative Trip Counter

Count total trips per zone—never resets.

```bash
make submit-job JOB=exercises/04-state/cumulative_counter.py
make db-query SQL="SELECT * FROM zone_trip_totals ORDER BY total_trips DESC LIMIT 10;"
```

**Solution:** [cumulative_counter.py](cumulative_counter.py)

### 2. ReducingState: Max Fare Tracker

Track the highest fare ever seen per zone.

```bash
make submit-job JOB=exercises/04-state/max_fare_tracker.py
make db-query SQL="SELECT * FROM zone_max_fare ORDER BY max_fare DESC LIMIT 10;"
```

**Solution:** [max_fare_tracker.py](max_fare_tracker.py)

### 3. State vs Window Comparison

Side-by-side: cumulative counter (state) vs hourly counter (window).

```bash
make submit-job JOB=exercises/04-state/state_vs_window.py
```

Compare outputs:
```sql
-- State: total keeps growing
SELECT zone, total_trips FROM zone_trip_totals WHERE zone = '161';

-- Window: resets each hour
SELECT zone, hourly_trips, window_end FROM zone_hourly_trips 
WHERE zone = '161' ORDER BY window_end DESC LIMIT 5;
```

**Solution:** [state_vs_window.py](state_vs_window.py)

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
