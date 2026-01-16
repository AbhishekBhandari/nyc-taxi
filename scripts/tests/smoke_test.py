from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("smoke").getOrCreate()
print("Spark version:", spark.version)

df = spark.range(5).toDF("id")
df.show()
spark.stop()
