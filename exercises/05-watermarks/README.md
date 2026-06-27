# 05 - Watermarks: Late Data Handling

**Goal:** Make watermarks tangible—see late data drop and recover.

## Concept

**Event time** = when the event happened  
**Processing time** = when Flink sees it  
**Watermark** = "I've seen all events up to this time"

Events arriving after the watermark passes their window are "late."

```
Timeline:
  Event time:      10:00    10:05    10:10    10:15
  Processing time: 10:02    10:07    10:20    10:16
                                      ^ late! (10:10 event arrived at 10:20)
  
  Watermark at 10:15 means: "Window [10:00-10:10] is closed"
  The 10:05 event arriving at 10:20 is LATE and dropped by default.
```

## Exercises

### 1. Delayed Producer

Modified producer that intentionally delays some events.

```bash
uv run exercises/05-watermarks/delayed_producer.py
```

This sends events with timestamps 5-30 minutes in the past, simulating out-of-order arrival.

**Solution:** [delayed_producer.py](delayed_producer.py)

### 2. Observe Late Data

Run a windowed aggregation and log watermark vs event time:

```bash
make submit-job JOB=exercises/05-watermarks/late_data_observer.py
make logs SVC=taskmanager  # Watch for "LATE EVENT" logs
```

Output shows: `event_time | watermark | processing_time | status`

**Solution:** [late_data_observer.py](late_data_observer.py)

### 3. Allowed Lateness

Configure the window to accept late events up to 10 minutes:

```python
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.allowed_lateness(Time.minutes(10))
```

Late events now UPDATE the window result instead of being dropped.

```bash
make submit-job JOB=exercises/05-watermarks/allowed_lateness.py
```

**Solution:** [allowed_lateness.py](allowed_lateness.py)

### 4. Side Output for Late Data

Route late events to a dead-letter topic instead of dropping:

```bash
make submit-job JOB=exercises/05-watermarks/side_output_late.py
make kafka-consume TOPIC=late-events LINES=5
```

**Solution:** [side_output_late.py](side_output_late.py)

## Key Insight

Print this for every event to understand watermark behavior:

```
| Event Time | Watermark  | Status    |
|------------|------------|-----------|
| 10:05:00   | 10:04:55   | on-time   |
| 10:03:00   | 10:05:00   | LATE      |
| 10:06:00   | 10:05:55   | on-time   |
```

## Watermark Strategies

```python
# Bounded out-of-orderness (most common)
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))

# Monotonically increasing (perfectly ordered data)
WatermarkStrategy.for_monotonous_timestamps()

# Custom (advanced)
WatermarkStrategy.for_generator(MyWatermarkGenerator())
```

## Verify

1. Run delayed producer + late data observer
2. See "LATE EVENT" logs in taskmanager
3. Run side output job, consume from `late-events` topic
4. Confirm late events captured instead of dropped

## Next

Proceed to [06-joins](../06-joins/) to combine multiple streams.
