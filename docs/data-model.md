# Data Model

## Silver schema (Yellow Taxi)
Key columns:
- `tpep_pickup_datetime` (timestamp)
- `tpep_dropoff_datetime` (timestamp)
- `PULocationID`, `DOLocationID` (int)
- `trip_distance`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount` (double)

Derived columns:
- `event_date` (date): `to_date(tpep_pickup_datetime)`
- `trip_duration_minutes` (long): diff between pickup/dropoff
- `fare_per_mile` (double): `fare_amount / trip_distance`

Ingestion metadata:
- `_ingested_at`, `_source_file`, `_source_system`, `ingest_month`

## Gold outputs
### Gold: pickup zone daily aggregates
Grain: `event_date` x pickup zone  
Metrics:
- `trip_count`
- total sums: `total_fare_amount`, `total_tip_amount`, `total_tolls_amount`, `total_amount`
- averages: `avg_trip_distance`, `avg_trip_duration_minutes`, `avg_fare_per_mile`
- percentiles: `p50_fare_per_mile`, `p95_fare_per_mile`

Note: `total_amount` may not equal `fare + tip + tolls` due to surcharges, taxes, and other charges. Include an extra field like `components_sum` to make this explicit.
