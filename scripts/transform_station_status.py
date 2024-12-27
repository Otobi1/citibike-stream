from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime

# POSTGRESQL connection parameters
POSTGRESQL_HOST = "postgres"
POSTGRESQL_PORT = "5432"
POSTGRES_DB = "citibike_db"
POSTGRES_USER = "citibike_user"
POSTGRES_PASSWORD = "citibike_pass"

# JDBC URL
jdbc_url = f"jdbc:postgresql://{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRES_DB}"

# Connection properties
connection_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Initialize Spark Session
spark = (SparkSession.builder
         .appName("TransformCitiBikeStationStatus")
         .getOrCreate())

# Read from PostgreSQL
df = spark.read \
    .jdbc(url=jdbc_url,
          table="station_status",
          properties=connection_properties
          )

# Display schema
df.printSchema()

# Some transformations
df_transformed = df.withColumn("last_reported", from_unixtime(col("last_reported"))).drop("last_reported")
df_transformed = df_transformed.filter(col("is_renting") == True)

# Show some sample of the transformed data
df_transformed.show(5)

# Write the transformed data to PostgreSQL
df_transformed.write \
    .jdbc(url=jdbc_url,
          table="station_status_transformed",
          mode="append",
          properties=connection_properties
          )

# Stop the spark session
spark.stop()