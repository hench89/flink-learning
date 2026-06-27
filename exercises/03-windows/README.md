# 03 - Windows: Time-Based Aggregations

**Goal:** Understand tumbling, sliding, and session windows.

## Concept

Windows group records by time for aggregation:

| Type | Behavior | Use Case |
|------|----------|----------|
| **Tumbling** | Fixed, non-overlapping | Hourly reports |
| **Sliding** | Fixed, overlapping | Rolling averages |
| **Session** | Dynamic, gap-based | User activity sessions |

## Visual

```
Time:     |----1----|----2----|----3----|----4----|

Tumbling: [   1h    ][   1h    ][   1h    ][   1h    ]
          ^ resets   ^ resets   ^ resets

Sliding:  [   1h window    ]
              [   1h window    ]
                  [   1h window    ]
          ^ slides every 15min (overlap!)

Session:  [  trips  ]         [  trips  ]
          ^ until 30m gap     ^ new session after gap
```

## Exercises

### 1. Tumbling: Hourly Revenue

Total revenue per zone, resetting every hour.

```bash
make submit-job JOB=exercises/03-windows/tumbling_revenue.py
make db-query SQL="SELECT * FROM hourly_revenue ORDER BY window_end DESC LIMIT 5;"
```

**Solution:** [tumbling_revenue.py](tumbling_revenue.py)

### 2. Sliding: Rolling Average Fare

5-minute window, sliding every 1 minute. Shows smoothed trends.

```bash
make submit-job JOB=exercises/03-windows/sliding_average.py
make db-query SQL="SELECT * FROM rolling_avg_fare ORDER BY window_end DESC LIMIT 10;"
```

Compare row counts with tumbling—sliding produces more rows (overlapping windows).

**Solution:** [sliding_average.py](sliding_average.py)

### 3. Session: Driver Trip Clusters

Group trips per driver until 30-minute inactivity. Reveals natural "shift" patterns.

```bash
make submit-job JOB=exercises/03-windows/session_driver.py
make db-query SQL="SELECT * FROM driver_sessions ORDER BY session_end DESC LIMIT 5;"
```

**Solution:** [session_driver.py](session_driver.py)

## Aha Moment

Run all three on the same data stream. Compare:
- Tumbling: Fixed number of rows per hour
- Sliding: 4× more rows (15-min slide into 1-hr window)
- Session: Variable rows depending on activity gaps

## Verify

```sql
-- Compare row counts
SELECT 'tumbling' as type, COUNT(*) FROM hourly_revenue
UNION ALL
SELECT 'sliding', COUNT(*) FROM rolling_avg_fare
UNION ALL
SELECT 'session', COUNT(*) FROM driver_sessions;
```

## Next

Proceed to [04-state](../04-state/) when aggregations need to persist across windows.
