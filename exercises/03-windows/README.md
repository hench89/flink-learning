# 03 - Windows: Time-Based Aggregations

**Goal:** Understand tumbling, sliding, and session windows — and why sliding costs more.

## Concept

| Type | Behavior | Output Rate |
|------|----------|-------------|
| **Tumbling** | Fixed, non-overlapping | 1 row per window |
| **Sliding** | Fixed, overlapping | Many rows (windows overlap!) |
| **Session** | Dynamic, gap-based | 1 row per activity burst |

## Setup

```bash
make db-setup FILE=exercises/03-windows/setup_tables.sql
```

## Run

```bash
make submit-job JOB=exercises/03-windows/tumbling_revenue.py
make submit-job JOB=exercises/03-windows/sliding_average.py
make submit-job JOB=exercises/03-windows/session_driver.py
```

## Aha Moment

Wait 1 minute, then compare row counts:

```bash
make db-query SQL="SELECT 'tumbling' as type, COUNT(*) as rows FROM minute_revenue UNION ALL SELECT 'sliding', COUNT(*) FROM rolling_avg_fare UNION ALL SELECT 'session', COUNT(*) FROM driver_sessions;"
```

**Sliding has 6× more rows than tumbling.** Why?

### Tumbling: One row per window

```
Time:     [  minute 1  ][  minute 2  ][  minute 3  ]
Output:        ↓             ↓             ↓
           1 row         1 row         1 row
```

Each window closes, emits one row, done. **1 row per minute.**

### Sliding: One row per slide

```
Time:     |-------- minute 1 --------|-------- minute 2 --------|
Windows:  [=====5 min window=====]
              [=====5 min window=====]
                  [=====5 min window=====]
                      [=====5 min window=====]
                          [=====5 min window=====]
                              [=====5 min window=====]
          ↑    ↑    ↑    ↑    ↑    ↑
         10s  10s  10s  10s  10s  10s  (slides every 10 seconds)
```

A new window starts every 10 seconds. Each window emits a row when it closes. **6 rows per minute.**

### The cost insight

The slide interval controls how many rows you produce:

| Slide every... | Rows per minute |
|----------------|-----------------|
| 1 minute | 1 |
| 10 seconds | 6 |
| 1 second | 60 |

Someone picks a 1-second slide "for smoother dashboards" and gets 60× more rows in their database.

## Observe the Data

```bash
# Tumbling: one row per minute
make db-query SQL="SELECT window_start, window_end FROM minute_revenue WHERE zone_id=161 ORDER BY window_start LIMIT 5;"

# Sliding: one row every 10 seconds (6× more)
make db-query SQL="SELECT window_start, window_end FROM rolling_avg_fare WHERE zone_id=161 ORDER BY window_start LIMIT 5;"

# Session: closes after 10s of inactivity per driver
make db-query SQL="SELECT * FROM driver_sessions WHERE driver_id=1001 ORDER BY session_start LIMIT 5;"
```

## Solutions

- [tumbling_revenue.py](tumbling_revenue.py) — `TUMBLE(..., INTERVAL '1' MINUTE)`
- [sliding_average.py](sliding_average.py) — `HOP(..., INTERVAL '10' SECOND, INTERVAL '5' MINUTE)`
- [session_driver.py](session_driver.py) — `SESSION(..., INTERVAL '10' SECOND)`

## Next

[04-state](../04-state/) — when aggregations need to persist across windows.
