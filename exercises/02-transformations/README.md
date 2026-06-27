# 02 - Transformations: Stateless Operations

**Goal:** Learn map, filter, flatMap—operations that transform records without memory.

## Concept

Stateless transformations process each record independently:
- **filter:** Keep records matching a condition
- **map:** Transform each record 1:1
- **flatMap:** Transform each record to 0, 1, or many output records

## Exercises

### 1. Filter: High-Fare Rides

Only emit rides where `fare_amount > 20`.

```bash
make submit-job JOB=exercises/02-transformations/filter_rides.py
make kafka-consume TOPIC=high-fare-rides LINES=5
```

**Solution:** [filter_rides.py](filter_rides.py)

### 2. Map: Convert to EUR

Transform the schema:
- Convert `fare_amount` USD → EUR (×0.92)
- Rename `PULocationID` → `pickup_zone`

```bash
make submit-job JOB=exercises/02-transformations/map_to_eur.py
```

**Solution:** [map_to_eur.py](map_to_eur.py)

### 3. FlatMap: Split Events

Emit TWO events per ride:
- `{"type": "pickup", "zone": <pickup_zone>, "timestamp": <pickup_time>}`
- `{"type": "dropoff", "zone": <dropoff_zone>, "timestamp": <dropoff_time>}`

This is useful for zone-based analytics where you want to count entries/exits.

```bash
make submit-job JOB=exercises/02-transformations/flatmap_events.py
```

**Solution:** [flatmap_events.py](flatmap_events.py)

## PyFlink Gotcha

Type hints matter! Flink infers the output schema from your function's return type.

```python
# Bad: no type hint, Flink can't infer schema
def transform(row):
    return {"zone": row["PULocationID"]}

# Good: explicit type hint
def transform(row) -> dict:
    return {"zone": row["PULocationID"]}
```

## Verify

```bash
make kafka-consume TOPIC=high-fare-rides LINES=3
make kafka-consume TOPIC=rides-eur LINES=3
make kafka-consume TOPIC=zone-events LINES=6  # Should see pickup/dropoff pairs
```

## Next

Proceed to [03-windows](../03-windows/) to aggregate records over time.
