# ingest_tripdata.py

import os
import pandas as pd
import logging
import sys
from sqlalchemy import create_engine, text
from pathlib import Path

# Configure logging to both stdout and a log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ingest_tripdata.log')
    ]
)

# Configuration
CLEANED_DIR = os.path.expanduser("./data/processed_tripdata/cleaned_raw")
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_USER = "citibike_user"
POSTGRES_PASSWORD = "citibike_pass"
POSTGRES_DB = "citibike_db"

# Database connection string
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def create_tables(engine):
    """
    Creates the tripdata and ingestion_log tables if they don't exist.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("""
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
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ingestion_log (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) UNIQUE,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
        logging.info("Ensured that tripdata and ingestion_log tables exist.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        sys.exit(1)


def get_ingested_files(engine):
    """
    Retrieves a set of already ingested file names from the ingestion_log table.
    """
    try:
        query = "SELECT file_name FROM ingestion_log;"
        ingested_files = pd.read_sql(query, engine)['file_name'].tolist()
        logging.info(f"Already ingested {len(ingested_files)} file(s).")
        return set(ingested_files)
    except Exception as e:
        logging.error(f"Error fetching ingestion log: {e}")
        sys.exit(1)


def ingest_parquet(engine, file_path):
    """
    Ingests a single Parquet file into the tripdata table and logs the ingestion.
    """
    file_name = os.path.basename(file_path)
    try:
        df = pd.read_parquet(file_path)
        logging.info(f"Read parquet file {file_name} with {len(df)} records.")

        # Connect using SQLAlchemy and insert data
        df.to_sql('tripdata', engine, if_exists='append', index=False, method='multi')
        logging.info(f"Inserted {len(df)} records from {file_name} into PostgreSQL.")

        # Log ingestion
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ingestion_log (file_name) VALUES (:file_name)
                ON CONFLICT (file_name) DO NOTHING;
            """), {'file_name': file_name})
        logging.info(f"Logged ingestion of {file_name}.")
    except Exception as e:
        logging.error(f"Error ingesting {file_name}: {e}")


def main():
    """
    Main function to ingest all new Parquet files into PostgreSQL.
    """
    # Create SQLAlchemy engine
    try:
        engine = create_engine(DB_URL)
        logging.info("Database engine created successfully.")
    except Exception as e:
        logging.error(f"Error creating database engine: {e}")
        sys.exit(1)

    # Create tables if they don't exist
    create_tables(engine)

    # Get list of already ingested files
    ingested_files = get_ingested_files(engine)

    # Iterate over parquet files in the cleaned_raw directory recursively
    parquet_files = list(Path(CLEANED_DIR).glob("**/*.parquet"))
    if not parquet_files:
        logging.info("No Parquet files found to ingest.")
        sys.exit(0)

    for parquet_file in parquet_files:
        file_name = parquet_file.name
        if file_name in ingested_files:
            logging.info(f"File {file_name} already ingested. Skipping.")
            continue
        ingest_parquet(engine, parquet_file)

    logging.info("Data ingestion process completed.")


if __name__ == "__main__":
    main()
