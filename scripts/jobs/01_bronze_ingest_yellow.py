import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType
)

YELLOW_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest_month", required=True, help="YYYY-MM (e.g., 2024-01)")
    parser.add_argument("--format", choices=["parquet","csv"], default="parquet")
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("bronze_yellow").getOrCreate()

    out_base = args.output_path
    CONTAINER_PROJECT_DIR = "/opt/project"
    raw_base = f"{CONTAINER_PROJECT_DIR}/data/raw/bronze/nyc_taxi/yellow"

    raw_path = f"{raw_base}/ingest_month={args.ingest_month}/*"

    if args.format == "parquet":
        df_raw = spark.read.parquet(f"file:{raw_path}")
    else:
        df_raw = (
            spark.read
            .option("header", True)
            .schema(YELLOW_SCHEMA)  # enforce schema in bronze when reading CSV
            .csv(f"file:{raw_path}")
        )

    #now add the metadata
    df_bronze = (
        df_raw
        .withColumn("ingest_month", F.lit(args.ingest_month))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_system", F.lit("nyc_tlc_yellow"))
    )

    (
        df_bronze.write
        .mode("append")
        .partitionBy("ingest_month")
        .parquet(f"file:{out_base}")
    )

    print(f"Bronze ingest complete for {args.ingest_month}")
    spark.stop()

if __name__ == "__main__":
    main()