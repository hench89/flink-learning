# 07 - Capstone: Real-Time Dashboard

**Goal:** Combine windows, state, watermarks, and joins into one multi-output job.

## Requirements

Build a single Flink job that outputs to multiple PostgreSQL tables:

| Output | Technique | Table |
|--------|-----------|-------|
| Hourly revenue | Tumbling window | `dashboard_hourly_revenue` |
| Top 5 busiest zones | Stateful aggregation | `dashboard_top_zones` |
| Average trip duration | Keyed state | `dashboard_avg_duration` |
| Late event count | Watermark monitoring | `dashboard_late_events` |

## Architecture

```
                    ┌─────────────────┐
                    │  Kafka Source   │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
     ┌────────────┐  ┌────────────┐  ┌────────────┐
     │  Tumbling  │  │   State    │  │  Watermark │
     │   Window   │  │  Counter   │  │  Monitor   │
     └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
           │               │               │
           ▼               ▼               ▼
     ┌──────────┐    ┌──────────┐    ┌──────────┐
     │ hourly_  │    │ top_zones│    │  late_   │
     │ revenue  │    │ avg_dur  │    │  events  │
     └──────────┘    └──────────┘    └──────────┘
```

## Exercise

Implement `dashboard_job.py` that:

1. **Reads** from the taxi Kafka topic with event-time watermarks
2. **Branches** the stream into multiple processing paths
3. **Outputs** to four PostgreSQL tables

### Hints

**Branching a stream:**
```python
# Same source, multiple sinks
rides = env.from_source(kafka_source, ...)

# Branch 1: hourly revenue
rides.key_by(lambda r: r["zone"]) \
     .window(TumblingEventTimeWindows.of(Time.hours(1))) \
     .reduce(sum_fares) \
     .add_sink(revenue_sink)

# Branch 2: cumulative counter
rides.key_by(lambda r: r["zone"]) \
     .process(CumulativeCounter()) \
     .add_sink(counter_sink)
```

**Late event side output:**
```python
late_tag = OutputTag("late-events")

class WatermarkMonitor(ProcessFunction):
    def process_element(self, value, ctx):
        if ctx.timestamp() < ctx.timer_service().current_watermark():
            ctx.output(late_tag, value)
        else:
            yield value
```

## Verify

```bash
make submit-job JOB=exercises/07-project/dashboard_job.py

# Check all outputs
make db-query SQL="SELECT * FROM dashboard_hourly_revenue ORDER BY window_end DESC LIMIT 5;"
make db-query SQL="SELECT * FROM dashboard_top_zones LIMIT 5;"
make db-query SQL="SELECT * FROM dashboard_avg_duration LIMIT 5;"
make db-query SQL="SELECT * FROM dashboard_late_events;"
```

## Solution

**File:** [dashboard_job.py](dashboard_job.py)

## Extension Ideas

Once working, try:
- Add a 5th output: trips with suspiciously long durations (> 2 hours)
- Enrich with zone names (stream-table join from module 06)
- Add Grafana dashboards reading from the PostgreSQL tables

## Congratulations!

You've completed the Flink curriculum. You now understand:
- Stateless transformations (map, filter, flatMap)
- Time-based windowing (tumbling, sliding, session)
- Keyed state management
- Event-time processing and watermarks
- Stream joins

Next steps:
- Deploy to a real Flink cluster
- Explore Flink SQL for declarative queries
- Learn about savepoints for job upgrades
