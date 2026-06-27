# Flink Concepts

Reference for key concepts used throughout the exercises.

---

## Jobs

A **job** is a running streaming application on the Flink cluster. When you run `make submit-job`, Flink:

1. Parses your Python code
2. Creates a dataflow graph (source → transform → sink)
3. Deploys it to the Task Managers
4. Starts processing records continuously

The job **runs forever** until you cancel it. It's not like a batch script that finishes—it keeps consuming from Kafka and producing output as long as data arrives.

```bash
make job-status          # List running jobs
make kill-job JOB_ID=<id>  # Stop a job
make ui                  # See jobs in Flink dashboard
```

---

## Tables = Kafka Topics

In Flink SQL, a **table** is a logical view over a **Kafka topic**:

```sql
CREATE TABLE rides (           -- Table name (used in SQL)
    PULocationID INTEGER,
    ...
) WITH (
    'connector' = 'kafka',
    'topic' = 'rides',         -- Actual Kafka topic name
    ...
)
```

| SQL Operation | Kafka Operation |
|---------------|-----------------|
| `SELECT FROM rides` | Consume from `rides` topic |
| `INSERT INTO high_fare_rides` | Produce to `high-fare-rides` topic |

The table defines the **schema** (columns, types). The `'topic'` property specifies **which Kafka topic** it maps to. They don't have to match—you could name the table `source` while it reads from topic `rides`.

---

## Stateless vs Stateful

**Stateless** operations (module 02) process each record independently:
- filter, map, flatMap
- No memory between records
- Output depends only on the current input

**Stateful** operations (modules 03-06) remember things:
- Windows accumulate records over time
- State tracks values across records (counters, max values)
- Joins correlate records from different streams

---

## Watermarks

Watermarks tell Flink "I've seen all events up to this time." They're needed for:
- **Windows** — to know when a time window is complete
- **Event-time processing** — to handle out-of-order events

Not needed for stateless transforms (module 02).

Covered in detail in [module 05](../exercises/05-watermarks/).

---

## Checkpointing

Checkpointing saves job state periodically so it can recover from failures. Flink snapshots:
- Kafka consumer offsets (where you are in each topic)
- State (window contents, counters, etc.)

If a job crashes, it restarts from the last checkpoint—no data loss.

```python
env.enable_checkpointing(10_000)  # Checkpoint every 10 seconds
```

---

## Event Time vs Processing Time

| Time | Definition | Use Case |
|------|------------|----------|
| **Event time** | When the event actually happened (timestamp in the data) | Accurate analytics |
| **Processing time** | When Flink processes the event | Simpler, but results vary with delays |

Most production jobs use event time for correctness.
