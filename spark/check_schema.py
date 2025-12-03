#!/usr/bin/env python3
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Create Spark session
builder = SparkSession.builder.appName("CheckSchema")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read Gold table
df = spark.read.format("delta").load("/opt/spark/delta_tables/gold")

print("Gold table schema:")
df.printSchema()

print("\nSample data:")
df.show(3, truncate=False)

print(f"\nTotal records: {df.count()}")

spark.stop()
