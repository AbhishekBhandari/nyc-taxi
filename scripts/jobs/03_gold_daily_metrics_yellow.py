import argparse
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser()
    # Choose one incremental mode:
    p.add_argument("--event_date", default=None, help="YYYY-MM-DD")
    p.add_argument("--start_date", default=None, help="YYYY-MM-DD")
    p.add_argument("--end_date", default=None, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--ingest_month", default=None, help="YYYY-MM")
    return p.parse_args()

def build_spark(app_name: str):
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    # important so we are only overwriting the affected partitions
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def apply_incremental_filter(df, args):
    # Prefer event_date; else range; else ingest_month.
    if args.event_date:
        return df.filter(F.col("event_date") == F.lit(args.event_date))

    if args.start_date and args.end_date:
        # end_date inclusive
        return df.filter(
            (F.col("event_date") >= F.lit(args.start_date)) &
            (F.col("event_date") <= F.lit(args.end_date))
        )

    if args.ingest_month:
        # Derive month filter from event_date (yyyy-MM)
        return df.filter(F.date_format(F.col("event_date"), "yyyy-MM") == F.lit(args.ingest_month))

    # If nothing provided: process everything (fine for small local dev, but avoid in production)
    return df

def main():
    silver_path = "file:/home/jovyan/project/data/output/silver/nyc_taxi/yellow"
    gold_path = "file:/home/jovyan/project/data/output/gold/nyc_taxi/yellow_daily"

    args = parse_args()
    spark = build_spark("gold_daily_metrics_yellow")
    silver = spark.read.parquet(silver_path)

    #now select only the silver data based on the inputs
    silver_f = apply_incremental_filter(silver, args)

    gold_daily = (
        silver_f
        .groupBy("event_date")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.sum("tolls_amount").alias("total_tolls_amount"),
            F.sum("total_amount").alias("total_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.expr("percentile_approx(fare_per_mile, 0.50)").alias("p50_fare_per_mile"),
            F.expr("percentile_approx(fare_per_mile, 0.95)").alias("p95_fare_per_mile")
        )
        # Make it presentation-ready
        .select(
            "event_date",
            "trip_count",
            F.round("total_fare_amount", 2).alias("total_fare_amount"),
            F.round("total_tip_amount", 2).alias("total_tip_amount"),
            F.round("total_tolls_amount", 2).alias("total_tolls_amount"),
            F.round("total_amount", 2).alias("total_amount"),
            F.round("avg_trip_distance", 3).alias("avg_trip_distance"),
            F.round("avg_trip_duration_minutes", 2).alias("avg_trip_duration_minutes"),
            F.round("avg_fare_per_mile", 3).alias("avg_fare_per_mile"),
            F.round("p50_fare_per_mile", 3).alias("p50_fare_per_mile"),
            F.round("p95_fare_per_mile", 3).alias("p95_fare_per_mile"),
        )
    )
    # Write partitioned by event_date (dynamic overwrite makes it incremental-safe)
    (
        gold_daily.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(gold_path)
    )

    print("Gold daily metrics written to:", gold_path)
    spark.stop()


if __name__ == "__main__":
    main()