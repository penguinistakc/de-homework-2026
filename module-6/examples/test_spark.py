import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.conf.get('spark.master')}")

df = spark.range(10)
df.show()

spark.stop()