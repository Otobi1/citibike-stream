# ingest_tripdata.py
import math
import os
import shutil

import pandas as pd
import logging
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from pathlib import Path
import psycopg2
from psycopg2 import sql
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging to both stdout and a log file with timestamp and log level
LOG_FILE = os.getenv('INGEST_LOG_FILE', 'ingest_tripdata.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)

# Configuration
CLEANED_DIR = os.path.expanduser(os.getenv('CLEANED_DIR', "./data/processed_tripdata/cleaned_raw"))
POSTGRES_HOST = os.getenv('POSTGRES_HOST', "localhost")
POSTGRES_PORT = os.getenv('POSTGRES_PORT', "5432")
POSTGRES_USER = os.getenv('POSTGRES_USER', "citibike_user")
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', "citibike_pass")
POSTGRES_DB = os.getenv('POSTGRES_DB', "citibike_db")

# Database connection string
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Ingestion Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))  # Number of records per batch
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))     # Maximum number of retries for failed batches
BACKOFF_FACTOR = int(os.getenv('BACKOFF_FACTOR', 2))  # Exponential backoff factor

# Define expected schema (should match tripdata table)
EXPECTED_COLUMNS = [
    'ride_id',
    'rideable_type',
    'started_at',
    'ended_at',
    'start_station_name',
    'start_station_id',
    'end_station_name',
    'end_station_id',
    'start_lat',
    'start_lng',
    'end_lat',
    'end_lng',
    'member_casual',
    'month'  # Ensure 'month' column is included if present
]

# Define the number of worker threads
NUM_WORKERS = int(os.getenv('NUM_WORKERS', 4))  # Adjust based on CPU cores and I/O capacity

def create_tables(engine):
    """
    Creates the ingestion_log and failed_ingestion_log tables if they don't exist.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ingestion_log (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) UNIQUE,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS failed_ingestion_log (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(255) UNIQUE,
                    error_message TEXT,
                    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
        logging.info("Ensured that ingestion_log and failed_ingestion_log tables exist.")
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

def get_failed_ingested_files(engine):
    """
    Retrieves a set of files that previously failed to ingest.
    """
    try:
        query = "SELECT file_name FROM failed_ingestion_log;"
        failed_files = pd.read_sql(query, engine)['file_name'].tolist()
        logging.info(f"Already failed to ingest {len(failed_files)} file(s).")
        return set(failed_files)
    except Exception as e:
        logging.error(f"Error fetching failed ingestion log: {e}")
        sys.exit(1)

def copy_dataframe_to_postgres(df, table_name='tripdata'):
    """
    Uses PostgreSQLs COPY command to efficiently load data from a DataFrame.
    """
    try:
        # Convert DataFrame to CSV in memory
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()

        # Generate COPY command
        columns = ','.join(EXPECTED_COLUMNS)
        copy_sql = sql.SQL("""
            COPY {table} ({fields}) FROM STDIN WITH CSV
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(columns)
        )

        # Execute COPY command
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        logging.info(f"Successfully copied {len(df)} records to {table_name} using COPY.")

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error using COPY to ingest data into {table_name}: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            cursor.close()
            conn.close()
        raise

def ingest_parquet(engine, file_path):
    """
    Ingests a single Parquet file into the tripdata table and logs the ingestion.
    Implements batch processing and retry logic using COPY command.
    """
    file_name = os.path.basename(file_path)
    try:
        df = pd.read_parquet(file_path)
        logging.info(f"Read parquet file {file_name} with {len(df)} records.")

        total_records = len(df)
        if total_records == 0:
            logging.warning(f"No records found in {file_name}. Skipping ingestion.")
            return

        num_batches = math.ceil(total_records / BATCH_SIZE)
        logging.info(f"Starting ingestion of {total_records} records in {num_batches} batches.")

        for batch_num, start in enumerate(range(0, total_records, BATCH_SIZE), start=1):
            end = start + BATCH_SIZE
            batch_df = df.iloc[start:end]

            retries = 0
            while retries < MAX_RETRIES:
                try:
                    copy_dataframe_to_postgres(batch_df, table_name='tripdata')
                    logging.info(f"Batch {batch_num}: Inserted records {start} to {end} successfully.")
                    break  # Exit retry loop on success
                except Exception as e:
                    retries += 1
                    wait_time = BACKOFF_FACTOR ** retries
                    logging.error(f"Batch {batch_num}: Error inserting records {start} to {end}: {e}. "
                                  f"Retrying in {wait_time} seconds ({retries}/{MAX_RETRIES}).")
                    time.sleep(wait_time)
                    if retries == MAX_RETRIES:
                        logging.error(f"Batch {batch_num}: Failed to insert records {start} to {end} after {MAX_RETRIES} retries.")
                        # Log failed ingestion
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO failed_ingestion_log (file_name, error_message)
                                VALUES (:file_name, :error_message)
                                ON CONFLICT (file_name) DO NOTHING;
                            """), {'file_name': file_name, 'error_message': f"Batch {batch_num} failed after {MAX_RETRIES} retries."})
                        break  # Proceed to next batch or handle as needed

        # Log ingestion after successful insertion of all batches
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ingestion_log (file_name) VALUES (:file_name)
                ON CONFLICT (file_name) DO NOTHING;
            """), {'file_name': file_name})
        logging.info(f"Logged ingestion of {file_name}.")

    except Exception as e:
        logging.error(f"Error ingesting {file_name}: {e}")
        # Log failed ingestion
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO failed_ingestion_log (file_name, error_message)
                VALUES (:file_name, :error_message)
                ON CONFLICT (file_name) DO NOTHING;
            """), {'file_name': file_name, 'error_message': str(e)})
        # Optionally, move the failed Parquet file to a separate directory
        FAILED_DIR = os.path.expanduser("./data/failed_ingestion")
        os.makedirs(FAILED_DIR, exist_ok=True)
        shutil.move(file_path, os.path.join(FAILED_DIR, file_name))
        logging.info(f"Moved failed Parquet file {file_name} to {FAILED_DIR}.")

def main():
    """
    Main function to ingest all new Parquet files into PostgreSQL.
    """
    # Create SQLAlchemy engine with connection pooling
    try:
        engine = create_engine(
            DB_URL,
            pool_size=10,         # Adjust based on expected concurrency
            max_overflow=20,      # Additional connections allowed beyond pool_size
            pool_timeout=30,      # Seconds to wait before giving up on getting a connection
            pool_recycle=1800     # Seconds after which a connection is recycled
        )
        logging.info("Database engine created successfully.")
    except Exception as e:
        logging.error(f"Error creating database engine: {e}")
        sys.exit(1)

    # Create necessary tables
    create_tables(engine)

    # Get list of already ingested and failed Parquet files
    ingested_files = get_ingested_files(engine)
    failed_files = get_failed_ingested_files(engine)

    # Iterate over Parquet files in the cleaned_raw directory recursively
    parquet_files = list(Path(CLEANED_DIR).glob("**/*.parquet"))
    if not parquet_files:
        logging.info("No Parquet files found to ingest.")
        sys.exit(0)

    # Filter out already ingested and failed Parquet files
    parquet_files_to_ingest = [parquet_file for parquet_file in parquet_files if parquet_file.name not in ingested_files and parquet_file.name not in failed_files]
    if not parquet_files_to_ingest:
        logging.info("No new Parquet files to ingest.")
        sys.exit(0)

    # Ingest Parquet files in parallel
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_file = {executor.submit(ingest_parquet, engine, parquet_file): parquet_file for parquet_file in parquet_files_to_ingest}
        for future in as_completed(future_to_file):
            parquet_file = future_to_file[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Unhandled exception during ingestion of {parquet_file.name}: {e}")

    logging.info("Data ingestion process completed.")

if __name__ == "__main__":
    main()
