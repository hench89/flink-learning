# Flink Learning Curriculum

Learn Apache Flink through progressive exercises using NYC taxi data.

## Prerequisites

- Docker & Docker Compose
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager

## Quick Start

```bash
# Build Docker images (first time only)
# This creates a local Flink environment with PyFlink, Kafka connectors, and PostgreSQL drivers.
# Takes 5-10 minutes on first run; subsequent builds use cache.
make build

# Start infrastructure
make start

# Verify services are running
docker-compose ps

# Open Flink UI
make ui
```

## Architecture

```
┌──────────┐    ┌──────────┐    ┌───────┐    ┌────────────┐
│ Producer │───▶│ Redpanda │───▶│ Flink │───▶│ PostgreSQL │
│ (taxi)   │    │ (Kafka)  │    │  Job  │    │ (results)  │
└──────────┘    └──────────┘    └───────┘    └────────────┘
```

| Service | Port | Purpose |
|---------|------|---------|
| Redpanda | 9092 | Kafka-compatible broker |
| PostgreSQL | 5432 | Result storage |
| Flink JobManager | 8081 | Web UI + job coordination |
| Flink TaskManager | — | Job execution |

## Running Exercises

```bash
# Terminal 1: Start producing taxi data
make produce

# Terminal 2: Submit a job
make submit-job JOB=exercises/02-transformations/filter_rides.py

# Check job status
make job-status

# View output
make kafka-consume TOPIC=high-fare-rides LINES=5
```

## Makefile Commands

```bash
# Setup
make build              # Build Docker images (first time)
make start              # Start all services
make stop               # Stop all services
make clean              # Remove containers and volumes

# Jobs
make submit-job JOB=exercises/02-transformations/filter_rides.py
make job-status         # List running jobs
make kill-job JOB_ID=<id>

# Data
make produce            # Run taxi data producer
make kafka-topics       # List Kafka topics
make kafka-consume TOPIC=rides LINES=10

# Database
make db-connect         # PostgreSQL shell
make db-query SQL="SELECT * FROM hourly_revenue LIMIT 5;"

# Monitoring
make ui                 # Open Flink Web UI
make logs SVC=jobmanager
```

## Concepts

See [docs/concepts.md](docs/concepts.md) for background on jobs, tables, watermarks, and other Flink concepts.

## Curriculum

| Module | Focus | Time |
|--------|-------|------|
| [01-basics](exercises/01-basics/) | Environment setup, data flow observation | 30 min |
| [02-transformations](exercises/02-transformations/) | map, filter, flatMap | 1 hr |
| [03-windows](exercises/03-windows/) | Tumbling, sliding, session windows | 1.5 hr |
| [04-state](exercises/04-state/) | Keyed state, checkpointing | 1.5 hr |
| [05-watermarks](exercises/05-watermarks/) | Late data handling, event time | 1.5 hr |
| [06-joins](exercises/06-joins/) | Stream-stream, stream-table joins | 2 hr |
| [07-project](exercises/07-project/) | Capstone: real-time dashboard | 2 hr |

## Tips

- Keep `make logs SVC=taskmanager` running to catch job errors
- Use Flink UI (localhost:8081) to inspect job graphs and metrics
- Each exercise builds on the same infrastructure—no teardown between modules
