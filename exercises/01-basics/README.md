# 01 - Basics: Environment & Data Flow

**Goal:** Verify the stack works and understand how data flows before writing code.

## Exercises

### 1. Start Infrastructure

```bash
make start
```

Verify all containers are healthy:
```bash
cd ~/Repos/data-engineering-zoomcamp/07-streaming/workshop
docker compose ps
```

Expected: 4 services running (redpanda, postgres, jobmanager, taskmanager).

### 2. Inspect Kafka Topics

```bash
make kafka-topics
```

You should see the `taxi-rides` topic (or similar).

### 3. Run the Producer

In the workshop directory:
```bash
cd ~/Repos/data-engineering-zoomcamp/07-streaming/workshop
uv run src/producers/taxi_producer.py
```

Watch messages arrive:
```bash
make kafka-consume TOPIC=taxi-rides LINES=5
```

### 4. Submit a Flink Job

Submit the workshop's pass-through job:
```bash
# Check what jobs exist in the workshop
ls ~/Repos/data-engineering-zoomcamp/07-streaming/workshop/src/job/
```

Open Flink UI to see the job running:
```bash
make ui
```

### 5. Query Results

```bash
make db-connect
```

In psql:
```sql
\dt                           -- list tables
SELECT * FROM <table> LIMIT 5;
\q                            -- exit
```

## Verify Understanding

You should be able to answer:
- Where does data enter the pipeline? (Kafka topic)
- What transforms it? (Flink job)
- Where does it exit? (PostgreSQL table)
- How do you monitor job status? (Flink UI at :8081)

## Next

Once comfortable with the data flow, proceed to [02-transformations](../02-transformations/).
