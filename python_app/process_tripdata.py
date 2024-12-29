# scripts/process_tripdata.py

import os
import zipfile
import pandas as pd
from pathlib import Path
import logging
import sys
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Configuration
DOWNLOAD_DIR = "./data/downloaded_tripdata"
PROCESSED_DIR = "./data/processed_tripdata"
PARQUET_FILE = "combined_tripdata.parquet"

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)
logging.info(f"Processed directory set to: {PROCESSED_DIR}")


def extract_zip(file_path, extract_to):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for zip_info in zip_ref.infolist():
                if zip_info.filename.endswith('.csv') or zip_info.filename.endswith('.csv.zip'):
                    zip_ref.extract(zip_info, extract_to)
                    # If it's a .csv.zip, extract it further
                    if zip_info.filename.endswith('.csv.zip'):
                        nested_zip_path = os.path.join(extract_to, zip_info.filename)
                        with zipfile.ZipFile(nested_zip_path, 'r') as nested_zip_ref:
                            nested_zip_ref.extractall(extract_to)
                        os.remove(nested_zip_path)  # Remove the nested zip after extraction
        logging.info(f"Extracted {file_path} to {extract_to}")
    except zipfile.BadZipFile as e:
        logging.error(f"Bad zip file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error extracting {file_path}: {e}")


def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Read CSV file {file_path} with {len(df)} records.")

        # Data Cleaning: Handle missing values
        df = df.dropna(subset=['ride_id', 'started_at', 'ended_at'])
        df = df.fillna({'rideable_type': 'unknown', 'member_casual': 'unknown'})
        logging.info(f"Cleaned data in {file_path}.")

        # Convert timestamps
        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
        df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')

        # Drop rows with invalid timestamps
        df = df.dropna(subset=['started_at', 'ended_at'])

        # Ensure numerical fields are correct
        numerical_fields = ['start_lat', 'start_lng', 'end_lat', 'end_lng']
        for field in numerical_fields:
            df[field] = pd.to_numeric(df[field], errors='coerce')
        df = df.dropna(subset=numerical_fields)

        # Add month column for partitioning
        df['month'] = df['started_at'].dt.to_period('M').astype(str)

        return df
    except Exception as e:
        logging.error(f"Error processing CSV file {file_path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure


def save_parquet(df, month):
    try:
        month_dir = os.path.join(PROCESSED_DIR, month)
        os.makedirs(month_dir, exist_ok=True)
        output_file = os.path.join(month_dir, f"tripdata_{month}.parquet")
        df.to_parquet(output_file, index=False)
        logging.info(f"Saved data for {month} to {output_file}")
    except Exception as e:
        logging.error(f"Error saving parquet for {month}: {e}")


def main():
    zip_files = list(Path(DOWNLOAD_DIR).glob("*.zip"))
    if not zip_files:
        logging.info("No zip files found to process.")
        sys.exit(0)

    for zip_file in zip_files:
        extract_zip(zip_file, PROCESSED_DIR)

    # Find all CSV files after extraction
    csv_files = list(Path(PROCESSED_DIR).glob("*.csv"))
    if not csv_files:
        logging.warning("No CSV files found after extraction.")
        sys.exit(0)

    for csv_file in csv_files:
        df = process_csv(csv_file)
        if not df.empty:
            # Save as parquet by month
            for month, group in df.groupby('month'):
                save_parquet(group, month)

    logging.info("Data processing completed.")


if __name__ == "__main__":
    main()