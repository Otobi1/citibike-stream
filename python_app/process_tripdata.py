# process_tripdata.py

import os
import zipfile
import pandas as pd
from pathlib import Path
import logging
import sys
import re
import uuid

# Configure logging to both stdout and a log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('process_tripdata.log')
    ]
)

# Configuration
DOWNLOAD_DIR = os.path.expanduser("./data/downloaded_tripdata")
EXTRACTED_DIR = os.path.expanduser("./data/processed_tripdata/extracted_raw")
CLEANED_DIR = os.path.expanduser("./data/processed_tripdata/cleaned_raw")
MAX_RETRIES = 3
BACKOFF_FACTOR = 2

# Define expected schema
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
    'member_casual'
]

# Ensure processed directories exist
os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)
logging.info(f"Extracted directory set to: {EXTRACTED_DIR}")
logging.info(f"Cleaned Parquet directory set to: {CLEANED_DIR}")


def extract_zip(file_path, extract_to):
    """
    Extracts zip files. Handles nested zip files ending with .csv.zip.
    """
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for zip_info in zip_ref.infolist():
                if zip_info.filename.endswith('.csv') or zip_info.filename.endswith('.csv.zip'):
                    zip_ref.extract(zip_info, extract_to)
                    extracted_file_path = os.path.join(extract_to, zip_info.filename)
                    # If it's a .csv.zip, extract it further
                    if zip_info.filename.endswith('.csv.zip'):
                        with zipfile.ZipFile(extracted_file_path, 'r') as nested_zip_ref:
                            nested_zip_ref.extractall(extract_to)
                        os.remove(extracted_file_path)  # Remove the nested zip after extraction
        logging.info(f"Extracted {file_path} to {extract_to}")
    except zipfile.BadZipFile as e:
        logging.error(f"Bad zip file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error extracting {file_path}: {e}")


def process_csv(file_path):
    """
    Processes a single CSV file:
    - Reads the CSV
    - Ensures it adheres to the expected schema
    - Fills missing data appropriately
    - Logs descriptive analytics
    - Returns the cleaned DataFrame
    """
    try:
        # To handle mixed types warning, set low_memory=False
        df = pd.read_csv(file_path, low_memory=False)
        logging.info(f"Read CSV file {file_path} with {len(df)} records.")

        # Ensure all expected columns are present
        missing_columns = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_columns:
            logging.warning(f"File {file_path} is missing columns: {missing_columns}. Adding them with default values.")
            for col in missing_columns:
                if col in ['started_at', 'ended_at']:
                    df[col] = pd.NaT  # Initialize with Not-a-Time for datetime columns
                elif col in ['start_lat', 'start_lng', 'end_lat', 'end_lng']:
                    df[col] = pd.NA  # Initialize with NA for numerical columns
                else:
                    df[col] = 'unknown'  # Initialize with 'unknown' for categorical columns

        # Reorder and select only expected columns
        df = df[EXPECTED_COLUMNS]

        # Convert timestamps
        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
        df['ended_at'] = pd.to_datetime(df['ended_at'], errors='coerce')

        # Add month column for partitioning
        df['month'] = df['started_at'].dt.to_period('M').astype(str)

        # Descriptive Analytics Logging

        # Total trips per month
        total_trips_per_month = df.groupby('month').size()
        logging.info("Total trips per month:")
        logging.info(f"\n{total_trips_per_month}")

        # Trips per month with no ride_id
        trips_no_ride_id = df[df['ride_id'].isna()].groupby('month').size()
        logging.info("Trips per month with no ride_id:")
        logging.info(f"\n{trips_no_ride_id}")

        # Trips per month with no started_at or ended_at
        trips_no_start_end = df[df['started_at'].isna() | df['ended_at'].isna()].groupby('month').size()
        logging.info("Trips per month with no started_at or ended_at:")
        logging.info(f"\n{trips_no_start_end}")

        # Handle Missing Data by Filling

        # Fill missing 'ride_id' with a unique UUID
        missing_ride_id_count = df['ride_id'].isna().sum()
        if missing_ride_id_count > 0:
            logging.info(f"Filling {missing_ride_id_count} missing ride_id(s) with UUIDs.")
            df['ride_id'] = df['ride_id'].fillna(
                pd.Series([str(uuid.uuid4()) for _ in range(missing_ride_id_count)],
                          index=df[df['ride_id'].isna()].index)
            )

        # Fill missing 'started_at' and 'ended_at' with a default timestamp or inferred value
        if df['started_at'].isna().any():
            earliest_start = df['started_at'].min()
            default_start = earliest_start if pd.notna(earliest_start) else pd.Timestamp('2000-01-01')
            missing_started_at_count = df['started_at'].isna().sum()
            logging.info(f"Filling {missing_started_at_count} missing started_at(s) with default value: {default_start}")
            df['started_at'] = df['started_at'].fillna(default_start)

        if df['ended_at'].isna().any():
            earliest_end = df['ended_at'].min()
            default_end = earliest_end if pd.notna(earliest_end) else pd.Timestamp('2000-01-01')
            missing_ended_at_count = df['ended_at'].isna().sum()
            logging.info(f"Filling {missing_ended_at_count} missing ended_at(s) with default value: {default_end}")
            df['ended_at'] = df['ended_at'].fillna(default_end)

        # Fill missing categorical fields with 'unknown'
        categorical_fields = ['rideable_type', 'member_casual',
                              'start_station_name', 'start_station_id',
                              'end_station_name', 'end_station_id']
        df[categorical_fields] = df[categorical_fields].fillna('unknown')

        # Fill missing numerical fields with the mean of the column
        numerical_fields = ['start_lat', 'start_lng', 'end_lat', 'end_lng']
        for field in numerical_fields:
            if df[field].isna().any():
                mean_value = df[field].mean()
                missing_count = df[field].isna().sum()
                logging.info(f"Filling {missing_count} missing {field}(s) with mean value: {mean_value}")
                df[field] = df[field].fillna(mean_value)

        # Ensure data types are consistent
        df['ride_id'] = df['ride_id'].astype(str)
        df['rideable_type'] = df['rideable_type'].astype(str)
        df['member_casual'] = df['member_casual'].astype(str)
        df['start_station_name'] = df['start_station_name'].astype(str)
        df['start_station_id'] = df['start_station_id'].astype(str)
        df['end_station_name'] = df['end_station_name'].astype(str)
        df['end_station_id'] = df['end_station_id'].astype(str)

        # Log the number of missing values after filling
        total_missing_after = df.isna().sum()
        logging.info(f"Missing values after filling:\n{total_missing_after}")

        # Verify Schema Consistency
        if list(df.columns) != EXPECTED_COLUMNS + ['month']:
            logging.warning(f"Schema mismatch in {file_path}. Expected columns: {EXPECTED_COLUMNS + ['month']}, "
                            f"but got: {list(df.columns)}")

        return df

    except Exception as e:
        logging.error(f"Error processing CSV file {file_path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure


def save_parquet(df, month):
    """
    Saves the DataFrame as a Parquet file partitioned by month.
    """
    try:
        output_file = os.path.join(CLEANED_DIR, month, f"tripdata_{month}.parquet")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_parquet(output_file, index=False)
        logging.info(f"Saved data for {month} to {output_file}")
    except Exception as e:
        logging.error(f"Error saving parquet for {month}: {e}")


def main():
    """
    Main function to process all downloaded zip files.
    """
    zip_files = list(Path(DOWNLOAD_DIR).glob("*.zip"))
    if not zip_files:
        logging.info("No zip files found to process.")
        sys.exit(0)

    for zip_file in zip_files:
        extract_zip(zip_file, EXTRACTED_DIR)

    # Find all CSV files after extraction (recursively if nested directories)
    csv_files = list(Path(EXTRACTED_DIR).rglob("*.csv"))
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