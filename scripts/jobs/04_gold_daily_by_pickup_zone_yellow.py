import argparse
from pyspark.sql import SparkSession, functions as F, types as T


def parse_args():
    p = argparse.ArgumentParser(description="Gold: daily metrics by pickup zone (Yellow Taxi)")

    # incremental controls (choose one)
    p.add_argument("--event_date", default=None, help="YYYY-MM-DD")
    p.add_argument("--start_date", default=None, help="YYYY-MM-DD")
    p.add_argument("--end_date", default=None, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--ingest_month", default=None, help="YYYY-MM")

    return p.parse_args()

def build_spark(app_name: str, driver_memory: str = "4g", shuffle_partitions: str = '32') -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory",driver_memory)
        .getOrCreate()
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    return spark

def apply_incremental_filter(df, args):
    if args.event_date:
        return df.filter(F.col("event_date") == F.lit(args.event_date))

    if args.start_date and args.end_date:
        return df.filter(
            (F.col("event_date") >= F.lit(args.start_date)) &
            (F.col("event_date") <= F.lit(args.end_date))
        )

    if args.ingest_month:
        # event_date is DATE; filter month via date_format
        return df.filter(F.date_format(F.col("event_date"), "yyyy-MM") == F.lit(args.ingest_month))

    return df

def read_zones(spark: SparkSession, zones_path: str):
    zones = (
        spark.read
        .option("header", True)
        .csv(zones_path)
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("Borough").alias("pu_borough"),
            F.col("Zone").alias("pu_zone"),
            F.col("service_zone").alias("pu_service_zone"),
        )
        .dropna(subset=["LocationID"])
        .dropDuplicates(subset=["LocationID"])
    )
    return zones

def main():
    args = parse_args()
    silver_path = "file:/home/jovyan/project/data/output/silver/nyc_taxi/yellow"
    gold_path = "file:/home/jovyan/project/data/output/gold/nyc_taxi/yellow_by_pickup_zone"
    zones_path = "file:/home/jovyan/project/data/dim/taxi_zone_lookup.csv"

    driver_memory = "4g"
    shuffle_partitions = "32"
    #build the session
    spark = build_spark("gold_daily_by_pickup_zone_yellow", driver_memory, shuffle_partitions)

    silver = spark.read.parquet(silver_path)

    required_cols = {
        "event_date", "PULocationID",
        "trip_distance", "trip_duration_minutes",
        "fare_amount", "tip_amount", "tolls_amount", "total_amount",
        "fare_per_mile",
    }
    missing = sorted(list(required_cols - set(silver.columns)))
    if missing:
        raise ValueError(f"Silver is missing required columns: {missing}")

    # Incremental slice first (reduces join/agg size)
    silver_f = apply_incremental_filter(silver, args)

    zones = read_zones(spark, zones_path)

    joined = (
        silver_f
        .join(F.broadcast(zones), silver_f["PULocationID"] == zones["LocationID"], "left")
        .drop("LocationID")
    )
    #handle the unknown zones cleanly
    joined = (
        joined
        .withColumn("pu_borough", F.coalesce(F.col("pu_borough"), F.lit("UNKNOWN")))
        .withColumn("pu_zone", F.coalesce(F.col("pu_zone"), F.lit("UNKNOWN")))
        .withColumn("pu_service_zone", F.coalesce(F.col("pu_service_zone"), F.lit("UNKNOWN")))
    )

    gold = (
        joined
        .groupBy("event_date", "pu_borough", "pu_zone", "pu_service_zone")
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
            F.expr("percentile_approx(fare_per_mile, 0.95)").alias("p95_fare_per_mile"),
        )
        .select(
            "event_date",
            "pu_borough", "pu_zone", "pu_service_zone",
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

    (
        gold.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(gold_path)
    )

    print("Gold daily-by-pickup-zone written to:", gold_path)
    spark.stop()


if __name__ == "__main__":
    main()
