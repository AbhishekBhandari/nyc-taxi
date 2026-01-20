import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    #first parse the ingest_month
    parser = argparse.ArgumentParser(description='Silver cleaning job for NYC Taxi Yellow data')
    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--ingest_month", required=True, help="YYYY-MM (e.g., 2024-01)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("silver_clean_yellow")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )

    ## setting partitionOverwriteMode to dynamic is important in the incremental jobs ow
    ## when a new month like 2025-01 is run it will replace the existing data
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", "32")


    bronze_path = f"file:{args.bronze_path}/ingest_month={args.ingest_month}"
    silver_path = f"file:{args.silver_path}"

    bronze = spark.read.parquet(bronze_path)


    silver = (  #perform basic filteration
        bronze
        .withColumn("ingest_month", F.lit(args.ingest_month))
        #basic validity filters
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") >= 0)
    )

    #now derive metrics
    silver = (
        silver
        # derive event_date (query-friendly) using partitioning
        .withColumn("event_date", F.to_date("tpep_pickup_datetime"))
        ## add logic to filter wrong dates generate
        .withColumn("event_month", F.date_format(F.col("event_date"), "yyyy-MM"))
        .filter(F.col("event_month") == F.col("ingest_month"))
        .drop("event_month")

        # derive trip duration
        .withColumn(
            "trip_duration_minutes",
            F.expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
        )
        #filter based on trip duration
        .filter(F.col("trip_duration_minutes") > 0)
        # derived metrics
        .withColumn(
            "fare_per_mile",
            F.round(F.col("fare_amount") / F.col("trip_distance"), 3)
        )
    )

    #lets save the files now
    (
        silver.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(silver_path)
    )
    print(f"Silver layer written successfully for {args.ingest_month}")
    spark.stop()

if __name__ == "__main__":

    main()