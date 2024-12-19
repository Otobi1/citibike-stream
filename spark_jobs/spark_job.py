from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import json
import psycopg2
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CitiBikeStationStatus") \
    .getOrCreate()

# Define schema based on Citi Bike API
schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("last_reported", IntegerType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "station_status") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col('json'), schema).alias('data')) \
    .select('data.*') \
    .withColumn("last_reported", (col('last_reported') / 1000).cast(TimestampType()))

# Write to PostgreSQL
def foreach_batch_function(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/citibike_db") \
        .option("dbtable", "station_status") \
        .option("user", "citibike_user") \
        .option("password", "citibike_pass") \
        .mode("append") \
        .save()

query = json_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()

query.awaitTermination()
