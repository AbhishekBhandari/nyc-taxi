# NYC Taxi Lakehouse (Spark + Airflow) — Bronze / Silver / Gold

End-to-end batch ETL project using **Apache Spark (PySpark)** for transformation and **Apache Airflow** for orchestration.  
Pipeline ingests NYC TLC Yellow Taxi data by `ingest_month`, writes **Bronze/Silver/Gold** layers to Parquet, and produces daily zone-level aggregates.

## What this project accomplish
- Spark DataFrame transformations (filtering, derived metrics, aggregations, percentiles)
- Partitioned Parquet layouts (query-friendly storage)
- Incremental processing by month
- Data quality checks in Silver
- Orchestration with Airflow + DockerOperator
- Reproducible local environment via Docker Compose

## Architecture (high level)
1. **Bronze**
   - Download TLC monthly data (data_source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
   - Ingest to Parquet with audit columns (e.g., `_ingested_at`, `_source_file`, `ingest_month`)
2. **Silver**
   - Validate/filter bad records
   - Add derived columns: `event_date`, `trip_duration_minutes`, `fare_per_mile`
3. **Gold**
   - Daily aggregates by pickup zone and borough
   - Joins to taxi zone dimension table

## Quickstart
### Prerequisites
- Docker + Docker Compose
- 8GB+ Docker memory recommended (Spark jobs can OOM at lower limits)

## Data source
This project uses the official NYC Taxi & Limousine Commission (TLC) trip record datasets:  
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


### 1) Start Spark dev container (optional)
```bash
docker compose -f docker/docker-compose-spark.yaml up -d
````
### 2) Start Airflow
```bash
cd docker
cp .env.example .env
docker compose -f docker-compose-airflow.yaml up -d
```

Airflow UI: http://localhost:8080

### 3) Run the DAG

Trigger nyc_taxi_yellow_pipeline with:
- ingest_month: YYYY-MM (e.g., 2024-01)

#### Data layers
- Bronze: raw-ish, typed, partitioned by ingest_month
- Silver: cleaned + derived metrics, partitioned by event_date
- Gold: analytics-ready aggregates

See:
- docs/architecture.md
- docs/data-model.md

## License
MIT