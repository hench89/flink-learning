# 01 - Basics: Environment & Data Flow

**Goal:** Verify the stack works and understand how data flows before writing code.

## Exercises

### 1. Start Infrastructure

```bash
make start
```

Verify all containers are healthy:
```bash
docker-compose ps
```

Expected: 4 services running (redpanda, postgres, jobmanager, taskmanager).

> **What's happening:** You're booting the foundational services—Redpanda (message broker), PostgreSQL (database), and Flink (stream processor). This creates an empty pipeline that data will flow through.

---

### 2. Inspect Kafka Topics

```bash
make kafka-topics
```

You should see an empty list (no topics yet).

> **Why this matters:** Confirms Kafka is running. In this local setup, topics are auto-created when the producer starts. In enterprise environments, topics are typically pre-created with strict configs—but auto-creation is fine for learning.

---

### 3. Run the Producer

In a separate terminal:
```bash
make produce
```

Watch messages arrive (Ctrl+C to stop):
```bash
make consumer-follow TOPIC=rides
```

> **What's happening:** `make produce` runs a **producer** that sends taxi ride events to Kafka. `consumer-follow` runs a **consumer** that reads them back—you're verifying messages are flowing through the broker.

---

### 4. Open Flink UI

```bash
make ui
```

Opens the Flink dashboard at http://localhost:8081.

**What you're seeing on the Overview page:**

- **Available Task Slots: 14** — How many parallel tasks Flink can run. Think of these as "workers" ready to process data.
- **Running Jobs: 0** — No streaming jobs yet (you'll add one in module 02)
- **Task Managers: 1** — We have one worker node with 15 slots

**Sidebar:**
- **Running Jobs** — Click a job to see its dataflow graph and metrics
- **Completed Jobs** — History of finished/failed jobs
- **Task Managers** — Memory and CPU usage per worker

> No jobs running yet—you'll submit your first in module 02.

---

### 5. Verify PostgreSQL Connectivity

PostgreSQL is a potential **sink** (output destination) for Flink jobs, but it's empty until jobs explicitly write to it.

```bash
# Quick query
make db-query SQL="SELECT tablename FROM pg_tables WHERE schemaname = 'public';"

# Or use pgcli for an interactive session (nicer autocomplete)
uvx pgcli -h localhost -U postgres -d postgres
# password: postgres
# then run: SELECT * FROM pg_tables WHERE schemaname = 'public';
```

**Expected:** Empty result set—that's correct!

> **Why it's empty:** The filter job outputs to Kafka, not PostgreSQL. Module 03+ jobs write aggregation results to PostgreSQL—you'll see tables appear then. This step just confirms the database is reachable.

---

## Data Flow Summary (What You've Verified)

```
Producer (step 3)
    ↓ writes to
Kafka topic: rides
    ↓ read by
Consumer (step 3)

Flink UI (step 4) ← ready, no jobs yet
PostgreSQL (step 5) ← empty, used as sink in later modules
```

In module 02, you'll add Flink jobs that sit between Kafka topics and transform the data.

## Verify Understanding

You should be able to answer:
- Where does data enter? → Kafka topic `rides` (from producer)
- What's Flink's role? → Processes/transforms streams (you'll use it in module 02)
- What's PostgreSQL's role? → Stores results (you'll use it in module 03+)
- How do you monitor? → Flink UI at localhost:8081

## Next

Proceed to [02-transformations](../02-transformations/) to write your own jobs.
