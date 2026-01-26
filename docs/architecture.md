# Architecture

## Compute & orchestration
- **PySpark** runs Spark jobs (DataFrame API) and delegates execution to the Spark JVM engine.
- **Airflow** orchestrates the pipeline steps using DockerOperator, launching short-lived containers per task.

## Storage layout (local dev)
All layers are written in Parquet to local disk (bind-mounted), using partitioning to enable:
- partition pruning
- smaller scans for date/month filters
- parallel reads

### Bronze
- Partition key: `ingest_month=YYYY-MM`
- Goal: preserve raw fields + add ingestion metadata

### Silver
- Partition key: `event_date=YYYY-MM-DD`
- Goal: enforce quality filters, add derived metrics, stable schema for consumers

### Gold
- Partition key: typically `event_date`
- Goal: aggregates for analytics (zone-level metrics)

## Why Parquet + partitioning?
Parquet stores column chunks and statistics enabling predicate pushdown; partitions allow directory-level pruning. Combined, filtering on partition columns (like `event_date`) avoids scanning unrelated data.
