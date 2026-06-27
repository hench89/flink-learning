# Flink Learning Curriculum

Learn Apache Flink through progressive exercises using NYC taxi data.

## Prerequisites

- Docker & Docker Compose
- Python 3.12
- [uv](https://github.com/astral-sh/uv) package manager

## Quick Start

```bash
# Clone the workshop (provides Docker infrastructure)
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
cd data-engineering-zoomcamp/07-streaming/workshop

# Start infrastructure
docker compose up -d

# Verify services
docker compose ps
```

Services running:
| Service | Port | Purpose |
|---------|------|---------|
| Redpanda | 9092 | Kafka-compatible broker |
| PostgreSQL | 5432 | Result storage |
| Flink JobManager | 8081 | Web UI + job coordination |
| Flink TaskManager | — | Job execution |

## Architecture

```
┌──────────┐    ┌──────────┐    ┌───────┐    ┌────────────┐
│ Producer │───▶│ Redpanda │───▶│ Flink │───▶│ PostgreSQL │
│ (taxi)   │    │ (Kafka)  │    │  Job  │    │ (results)  │
└──────────┘    └──────────┘    └───────┘    └────────────┘
```

## Makefile Commands

```bash
make start          # Start all services
make stop           # Stop all services
make logs           # Tail all logs
make logs SVC=jobmanager  # Tail specific service

make submit-job JOB=exercises/02-transformations/filter_rides.py
make job-status     # List running jobs
make kill-job JOB_ID=<id>

make kafka-topics   # List Kafka topics
make kafka-consume TOPIC=taxi-rides LINES=10

make db-connect     # PostgreSQL shell
make db-query SQL="SELECT * FROM aggregations LIMIT 5;"

make ui             # Open Flink Web UI
```

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

- Keep `make logs SVC=jobmanager` running in a terminal to catch errors
- Use Flink UI (localhost:8081) to inspect job graphs and metrics
- Each exercise builds on the same infrastructure—no teardown between modules
