from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"


spark = SparkSession.builder \
    .appName("EV-Analysis") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
    ) \
    .getOrCreate()


schema = StructType() \
    .add("City", StringType()) \
    .add("Model Year", IntegerType())

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "electric_vehicles") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

result = parsed \
    .filter(col("Model Year") == 2023) \
    .groupBy("City") \
    .count() \
    .orderBy(col("count").desc())

result.show(3)

spark.stop()
