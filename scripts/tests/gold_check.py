from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("check_gold").getOrCreate()
df = spark.read.parquet("file:/home/jovyan/project/data/output/gold/nyc_taxi/yellow_daily")
df.orderBy(F.col("event_date").desc()).show(20, truncate=False)
df.printSchema()