# scripts/ingest_tripdata.py

import os
import pandas as pd
import psycopg2
from psycopg2 import extras
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Configuration
PROCESSED_DIR = "../data/processed_tripdata"
PARQUET_FILE = "combined_tripdata.parquet"

POSTGRES_HOST = "localhost"  # Host is localhost since Docker port is mapped
POSTGRES_PORT = "5432"
POSTGRES_USER = "citibike_user"
POSTGRES_PASSWORD = "citibike_pass"
POSTGRES_DB = "citibike_db"


def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        conn.autocommit = True
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL successfully.")
        return conn, cursor
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)


def create_tables(cursor):
    # Create tripdata table
    create_tripdata_table = """
    CREATE TABLE IF NOT EXISTS tripdata (
        ride_id VARCHAR(255) PRIMARY KEY,
        rideable_type VARCHAR(50),
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        start_station_name VARCHAR(255),
        start_station_id VARCHAR(50),
        end_station_name VARCHAR(255),
        end_station_id VARCHAR(50),
        start_lat DOUBLE PRECISION,
        start_lng DOUBLE PRECISION,
        end_lat DOUBLE PRECISION,
        end_lng DOUBLE PRECISION,
        member_casual VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Create ingestion log table
    create_ingestion_log_table = """
    CREATE TABLE IF NOT EXISTS ingestion_log (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255) UNIQUE,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        cursor.execute(create_tripdata_table)
        cursor.execute(create_ingestion_log_table)
        logging.info("Ensured that tripdata and ingestion_log tables exist.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        sys.exit(1)


def ingest_data(cursor, df):
    try:
        # Replace NaNs with None
        df = df.where(pd.notnull(df), None)

        # Define insert query with ON CONFLICT to avoid duplicates
        insert_query = """
        INSERT INTO tripdata (
            ride_id,
            rideable_type,
            started_at,
            ended_at,
            start_station_name,
            start_station_id,
            end_station_name,
            end_station_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            member_casual
        ) VALUES %s
        ON CONFLICT (ride_id) DO NOTHING;
        """

        records = df[['ride_id', 'rideable_type', 'started_at', 'ended_at',
                      'start_station_name', 'start_station_id', 'end_station_name',
                      'end_station_id', 'start_lat', 'start_lng',
                      'end_lat', 'end_lng', 'member_casual']].values.tolist()

        extras.execute_values(cursor, insert_query, records, page_size=1000)
        logging.info(f"Inserted {len(records)} records into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {e}")


def main():
    # Connect to PostgreSQL
    conn, cursor = connect_postgres()

    # Create tables
    create_tables(cursor)

    # Read the Parquet file
    parquet_path = os.path.join(PROCESSED_DIR, PARQUET_FILE)
    if not os.path.exists(parquet_path):
        logging.error(f"Parquet file not found: {parquet_path}")
        sys.exit(1)

    try:
        df = pd.read_parquet(parquet_path)
        logging.info(f"Read Parquet file with {len(df)} records.")
    except Exception as e:
        logging.error(f"Error reading Parquet file: {e}")
        sys.exit(1)

    # Ingest data into PostgreSQL
    ingest_data(cursor, df)

    # Log ingestion
    try:
        insert_log_query = """
        INSERT INTO ingestion_log (file_name) VALUES (%s)
        ON CONFLICT (file_name) DO NOTHING;
        """
        cursor.execute(insert_log_query, (PARQUET_FILE,))
        logging.info(f"Logged ingestion of {PARQUET_FILE}.")
    except Exception as e:
        logging.error(f"Error logging ingestion of {PARQUET_FILE}: {e}")

    # Close connections
    cursor.close()
    conn.close()
    logging.info("Data ingestion process completed.")


if __name__ == "__main__":
    main()
