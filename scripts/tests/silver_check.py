from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("verify_constraints").getOrCreate()
silver_path = "file:/home/jovyan/project/data/output/silver/nyc_taxi/yellow"
df = spark.read.parquet(silver_path)

checks = {
    "null_pickup": df.filter(F.col("tpep_pickup_datetime").isNull()).count(),
    "null_dropoff": df.filter(F.col("tpep_dropoff_datetime").isNull()).count(),
    "nonpositive_distance": df.filter(F.col("trip_distance") <= 0).count(),
    "negative_fare": df.filter(F.col("fare_amount") < 0).count(),
    "nonpositive_duration": df.filter(F.col("trip_duration_minutes") <= 0).count()
}

print("Silver constraint violations:")
for k, v in checks.items():
    print(f"  {k}: {v}")

### lets check for summary stats
print("Summary stats:")
df.select(
    F.min("trip_duration_minutes").alias("min_duration"),
    F.expr("percentile_approx(trip_duration_minutes, 0.5)").alias("p50_duration"),
    F.expr("percentile_approx(trip_duration_minutes, 0.99)").alias("p99_duration"),
    F.max("trip_duration_minutes").alias("max_duration"),
    F.expr("percentile_approx(fare_per_mile, 0.5)").alias("p50_fare_per_mile"),
    F.expr("percentile_approx(fare_per_mile, 0.99)").alias("p99_fare_per_mile"),
).show(truncate=False)

print("\\nSample rows:")
df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_duration_minutes",
    "trip_distance",
    "fare_amount",
    "fare_per_mile",
    "event_date",
    "ingest_month",
).orderBy(F.col("event_date").desc()).show(10, truncate=False)


spark.stop()