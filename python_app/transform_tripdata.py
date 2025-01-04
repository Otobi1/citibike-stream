"""
PySpark ETL Script for Citibike Data

This script performs the following operations:
1. Connects to a PostgreSQL database and reads data from `station_status`, `station_information`, and `tripdata` tables.
2. Transforms and aggregates station status data.
3. Enriches trip data with detailed station information.
4. Loads the transformed data back into PostgreSQL tables: `station_status_transformed` and `agg_tripdata`.

Requirements:
- PySpark
- PostgreSQL JDBC Driver
- PostgreSQL database with appropriate tables and data.

Author: [tobi]
Date: [04.01.2025]
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date, max as spark_max, broadcast


def initialize_spark(jdbc_driver_path: str) -> SparkSession:
    """
    Initialize and return a SparkSession with the specified configurations.

    Args:
        jdbc_driver_path (str): Path to the PostgreSQL JDBC driver JAR file.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = SparkSession.builder \
        .appName("CitiBikeETL") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.driver.memory", "8g") \
        .config("spark.memory.fraction", "0.8") \
        .master("local[4]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    return spark


def get_connection_properties(user: str, password: str) -> dict:
    """
    Construct connection properties for JDBC.

    Args:
        user (str): PostgreSQL username.
        password (str): PostgreSQL password.

    Returns:
        dict: JDBC connection properties.
    """
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }


def read_table(spark: SparkSession, jdbc_url: str, table: str, properties: dict):
    """
    Read a table from PostgreSQL into a Spark DataFrame.

    Args:
        spark (SparkSession): Active Spark session.
        jdbc_url (str): JDBC URL for PostgreSQL connection.
        table (str): Name of the table to read.
        properties (dict): JDBC connection properties.

    Returns:
        DataFrame: Spark DataFrame containing the table data.
    """
    return spark.read.jdbc(url=jdbc_url, table=table, properties=properties)


def transform_station_data(spark: SparkSession, jdbc_url: str, properties: dict):
    """
    Transform and aggregate station status data.

    Args:
        spark (SparkSession): Active Spark session.
        jdbc_url (str): JDBC URL for PostgreSQL connection.
        properties (dict): JDBC connection properties.

    Returns:
        DataFrame: Aggregated station status DataFrame.
    """
    # Read station information
    station_info_df = spark.read \
        .jdbc(url=jdbc_url, table="station_information", properties=properties) \
        .select(
        "station_id",
        "name",
        "short_name",
        "lat",
        "lon",
        "region_id",
        "capacity",
        "eightd_has_key_dispenser",
        "has_kiosk",
        "installed",
        "last_reported",
        "created_at",
        "updated_at"
    ) \
        .cache()

    # Read station status
    station_status_df = spark.read \
        .jdbc(url=jdbc_url, table="station_status", properties=properties) \
        .select(
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
        "inserted_at"
    ) \
        .withColumnRenamed("last_reported", "status_last_reported")

    # Join station information with station status
    joined_df = station_status_df.join(
        station_info_df,
        on="station_id",
        how="inner"
    )

    # Aggregate station status data
    aggregated_df = joined_df.groupBy(
        "station_id",
        "name",
        "short_name",
        "region_id",
        "lat",
        "lon",
        "capacity",
        "has_kiosk",
        "installed",
        "is_installed",
        "is_returning",
        "is_renting"
    ).agg(
        avg("num_bikes_available").alias("avg_bikes_available"),
        avg("num_docks_available").alias("avg_docks_available"),
        spark_max("status_last_reported").alias("status_last_reported"),
        spark_max("last_reported").alias("info_last_reported")
    )

    # Add aggregation date
    aggregated_df = aggregated_df.withColumn(
        "aggregation_date", to_date(col("status_last_reported"))
    )

    return aggregated_df


def enrich_tripdata(spark: SparkSession, jdbc_url: str, properties: dict):
    """
    Enrich trip data with station information.

    Args:
        spark (SparkSession): Active Spark session.
        jdbc_url (str): JDBC URL for PostgreSQL connection.
        properties (dict): JDBC connection properties.

    Returns:
        DataFrame: Enriched trip data DataFrame.
    """
    # Read trip data
    tripdata_df = spark.read \
        .jdbc(url=jdbc_url, table="tripdata", properties=properties) \
        .select(
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
        "month"
    ) \
        .cache()

    # Rename original station columns
    tripdata_df = tripdata_df \
        .withColumnRenamed("start_station_name", "original_start_station_name") \
        .withColumnRenamed("start_lat", "original_start_lat") \
        .withColumnRenamed("end_station_name", "original_end_station_name") \
        .withColumnRenamed("end_lat", "original_end_lat")

    # Read transformed station status data
    transformed_station_status_df = spark.read \
        .jdbc(url=jdbc_url, table="station_status_transformed", properties=properties) \
        .select(
        "station_id",
        "name",
        "short_name",
        "region_id",
        "lat",
        "lon",
        "capacity",
        "has_kiosk",
        "installed",
        "is_installed",
        "is_returning",
        "is_renting",
        "avg_bikes_available",
        "avg_docks_available",
        "status_last_reported",
        "info_last_reported",
        "aggregation_date"
    ) \
        .cache()

    # Prepare Start Station DataFrame
    start_station_df = transformed_station_status_df.select(
        col("short_name").alias("start_station_short_name"),
        col("name").alias("start_station_name"),
        col("lat").alias("start_lat"),
        col("lon").alias("start_lon"),
        col("capacity").alias("start_capacity"),
        col("avg_bikes_available").alias("start_avg_bikes_available"),
        col("avg_docks_available").alias("start_avg_docks_available"),
        col("status_last_reported").alias("start_status_last_reported"),
        col("info_last_reported").alias("start_info_last_reported")
    )

    # Prepare End Station DataFrame
    end_station_df = transformed_station_status_df.select(
        col("short_name").alias("end_station_short_name"),
        col("name").alias("end_station_name"),
        col("lat").alias("end_lat"),
        col("lon").alias("end_lon"),
        col("capacity").alias("end_capacity"),
        col("avg_bikes_available").alias("end_avg_bikes_available"),
        col("avg_docks_available").alias("end_avg_docks_available"),
        col("status_last_reported").alias("end_status_last_reported"),
        col("info_last_reported").alias("end_info_last_reported")
    )

    # Broadcast station DataFrames for efficient joins
    start_station_broadcast = broadcast(start_station_df)
    end_station_broadcast = broadcast(end_station_df)

    # Join trip data with start station information
    trip_with_start = tripdata_df.join(
        start_station_broadcast,
        tripdata_df.start_station_id == start_station_broadcast.start_station_short_name,
        how="left"
    )

    # Join the result with end station information
    final_df = trip_with_start.join(
        end_station_broadcast,
        tripdata_df.end_station_id == end_station_broadcast.end_station_short_name,
        how="left"
    )

    return final_df


def main():
    # Configuration Parameters
    JDBC_DRIVER_PATH = "./python_app/jars/postgresql-42.5.4.jar"
    POSTGRESQL_HOST = "localhost"
    POSTGRESQL_PORT = "5432"
    POSTGRES_DB = "citibike_db"
    POSTGRES_USER = "citibike_user"
    POSTGRES_PASSWORD = "citibike_pass"

    # JDBC URL
    jdbc_url = f"jdbc:postgresql://{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRES_DB}"

    # Initialize Spark Session
    spark = initialize_spark(JDBC_DRIVER_PATH)

    # Get Connection Properties
    connection_properties = get_connection_properties(POSTGRES_USER, POSTGRES_PASSWORD)

    # -------------------- Extraction --------------------
    try:
        # Read DataFrames from PostgreSQL
        df_station_status = read_table(spark, jdbc_url, "station_status", connection_properties)
        df_station_info = read_table(spark, jdbc_url, "station_information", connection_properties)
        df_tripdata = read_table(spark, jdbc_url, "tripdata", connection_properties)

        # Display Schemas and Sample Data
        print("=== Station Status Schema ===")
        df_station_status.printSchema()
        print("=== Station Information Schema ===")
        df_station_info.printSchema()
        print("=== Trip Data Schema ===")
        df_tripdata.printSchema()

        print("=== Sample Station Status Data ===")
        df_station_status.show(5)
        print("=== Sample Station Information Data ===")
        df_station_info.show(5)
        print("=== Sample Trip Data ===")
        df_tripdata.show(5)

    except Exception as e:
        print(f"Error during data extraction: {e}")
        spark.stop()
        sys.exit(1)

    # -------------------- Transformation --------------------
    try:
        # Transform and aggregate station data
        aggregated_station_df = transform_station_data(spark, jdbc_url, connection_properties)
        print("=== Aggregated Station Data ===")
        aggregated_station_df.show(5)
        aggregated_station_df.printSchema()

        # Create Temporary View for Verification
        aggregated_station_df.createOrReplaceTempView("station_status_transformed")

        # Verification Query
        dim_station_status = spark.sql("""
            SELECT COUNT(*) AS total_records
            FROM station_status_transformed
        """)
        print("=== Total Records in Transformed Station Status ===")
        dim_station_status.show()

    except Exception as e:
        print(f"Error during data transformation: {e}")
        spark.stop()
        sys.exit(1)

    # -------------------- Loading Transformed Station Data --------------------
    try:
        # Write aggregated station data back to PostgreSQL
        aggregated_station_df.write \
            .jdbc(url=jdbc_url,
                  table="station_status_transformed",
                  mode="append",
                  properties=connection_properties
                  )
        print("Successfully wrote aggregated station data to 'station_status_transformed' table.")
    except Exception as e:
        print(f"Error during loading transformed station data: {e}")
        spark.stop()
        sys.exit(1)

    # -------------------- Enrichment of Trip Data --------------------
    try:
        # Enrich trip data with station information
        final_trip_df = enrich_tripdata(spark, jdbc_url, connection_properties)
        print("=== Enriched Trip Data ===")
        final_trip_df.show(5)
        final_trip_df.printSchema()
    except Exception as e:
        print(f"Error during trip data enrichment: {e}")
        spark.stop()
        sys.exit(1)

    # -------------------- Loading Enriched Trip Data --------------------
    try:
        # Write enriched trip data back to PostgreSQL
        final_trip_df.write \
            .jdbc(url=jdbc_url,
                  table="agg_tripdata",
                  mode="append",
                  properties=connection_properties
                  )
        print("Successfully wrote enriched trip data to 'agg_tripdata' table.")
    except Exception as e:
        print(f"Error during loading enriched trip data: {e}")
        spark.stop()
        sys.exit(1)

    # -------------------- Completion --------------------
    print("ETL Process Completed Successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
