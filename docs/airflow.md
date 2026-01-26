# Airflow Orchestration

## DAG: nyc_taxi_yellow_pipeline
Pipeline stages:
- Bronze: download -> ingest
- Silver: clean/derive -> validate
- Gold: aggregates (zone/day)

## Parameters
- `ingest_month`: `YYYY-MM` (e.g. `2024-01`)

## LocalExecutor vs CeleryExecutor
This project uses **LocalExecutor** to minimize infra for local development.

## Airflow configuration
This project uses environment variables for most settings. Airflow can also read from `airflow.cfg` located under `$AIRFLOW_HOME` by default, and the location can be overridden via `AIRFLOW_CONFIG`.