import os
import zipfile
import pandas as pd
from pathlib import Path
import logging
import sys
import uuid
import time
import math
import shutil

# Configure logging to both stdout and a log file with timestamp and log level
LOG_FILE = os.getenv('PROCESS_LOG_FILE', 'process_tripdata.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)

# Configuration
DOWNLOAD_DIR = os.path.expanduser(os.getenv('DOWNLOAD_DIR', "./data/downloaded_tripdata"))
EXTRACTED_DIR = os.path.expanduser(os.getenv('EXTRACTED_DIR', "./data/processed_tripdata/extracted_raw"))
CLEANED_DIR = os.path.expanduser(os.getenv('CLEANED_DIR', "./data/processed_tripdata/cleaned_raw"))

# Retry Configuration
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
BACKOFF_FACTOR = int(os.getenv('BACKOFF_FACTOR', 2))

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

# Target number of records per Parquet file
TARGET_RECORDS_PER_PARQUET = int(os.getenv('TARGET_RECORDS_PER_PARQUET', 200000))  # Adjust as needed

def retry_operation(operation, max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR, *args, **kwargs):
    """
    Retries a given operation with exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            wait_time = backoff_factor ** attempt
            logging.error(f"Attempt {attempt} failed with error: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    logging.error(f"Operation failed after {max_retries} attempts.")
    raise Exception(f"Operation failed after {max_retries} attempts.")

def extract_zip(file_path, extract_to):
    """
    Extracts zip files. Handles nested zip files ending with .csv.zip.
    """
    def perform_extraction():
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

    retry_operation(perform_extraction)

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
        # Read CSV with low_memory=False to handle mixed types
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
        return pd.DataFrame()

def save_parquet_partitioned(df, month, target_records=TARGET_RECORDS_PER_PARQUET):
    """
    Saves the DataFrame as Parquet files partitioned by month and split into chunks with similar sizes.
    """
    try:
        # Calculate the number of chunks needed
        total_records = len(df)
        num_chunks = math.ceil(total_records / target_records)
        if num_chunks == 0:
            num_chunks = 1

        logging.info(f"Saving data for {month} into {num_chunks} Parquet file(s) with up to {target_records} records each.")

        for chunk_num in range(num_chunks):
            start = chunk_num * target_records
            end = start + target_records
            chunk_df = df.iloc[start:end]

            # Define output file path
            output_file = os.path.join(CLEANED_DIR, month, f"tripdata_{month}_part{chunk_num + 1}.parquet")
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Save as Parquet with compression for efficiency
            chunk_df.to_parquet(output_file, index=False, compression='snappy')
            logging.info(f"Saved Parquet file: {output_file} with {len(chunk_df)} records.")

    except Exception as e:
        logging.error(f"Error saving Parquet files for {month}: {e}")

def cleanup_extracted_files(extract_to):
    """
    Cleans up extracted CSV files after processing.
    """
    try:
        shutil.rmtree(extract_to)
        logging.info(f"Cleaned up extracted files in {extract_to}.")
    except Exception as e:
        logging.error(f"Error cleaning up extracted files in {extract_to}: {e}")

def move_processed_zip(file_path):
    """
    Moves a successfully processed ZIP file to the PROCESSED_DIR.
    """
    PROCESSED_DIR = os.path.expanduser("./data/processed_processing")
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    shutil.move(file_path, os.path.join(PROCESSED_DIR, os.path.basename(file_path)))
    logging.info(f"Moved processed ZIP file {file_path} to {PROCESSED_DIR}.")

def move_failed_zip(file_path):
    """
    Moves a failed ZIP file to the FAILED_DIR.
    """
    FAILED_DIR = os.path.expanduser("./data/failed_processing")
    os.makedirs(FAILED_DIR, exist_ok=True)
    shutil.move(file_path, os.path.join(FAILED_DIR, os.path.basename(file_path)))
    logging.info(f"Moved failed ZIP file {file_path} to {FAILED_DIR}.")

def process_zip_file(zip_file_path):
    """
    Processes a single ZIP file: extracts, processes CSVs, saves Parquet, and cleans up.
    """
    file_name = os.path.basename(zip_file_path)
    try:
        logging.info(f"Processing ZIP file: {file_name}")
        extract_zip(zip_file_path, EXTRACTED_DIR)

        # Find all CSV files after extraction (recursively)
        csv_files = list(Path(EXTRACTED_DIR).rglob("*.csv"))
        if not csv_files:
            logging.warning(f"No CSV files found in {zip_file_path}.")
            return

        for csv_file in csv_files:
            logging.info(f"Processing CSV file: {csv_file}")
            df = process_csv(csv_file)
            if not df.empty:
                # Save as Parquet partitioned by month and chunked
                for month, group in df.groupby('month'):
                    save_parquet_partitioned(group, month)
            else:
                logging.warning(f"No data to save for CSV file: {csv_file}")

        # Cleanup extracted CSV files
        cleanup_extracted_files(EXTRACTED_DIR)

        # Move processed ZIP to PROCESSED_DIR
        move_processed_zip(zip_file_path)

    except Exception as e:
        logging.error(f"Error processing ZIP file {file_name}: {e}")
        # Optionally, implement additional error handling like logging to a failed processing log
        move_failed_zip(zip_file_path)

def main():
    """
    Main function to process all downloaded ZIP files.
    """
    # Ensure processed directories exist
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    os.makedirs(CLEANED_DIR, exist_ok=True)

    # Iterate over ZIP files in the download directory recursively
    zip_files = list(Path(DOWNLOAD_DIR).glob("**/*.zip"))
    if not zip_files:
        logging.info("No ZIP files found to process.")
        sys.exit(0)

    # Define the number of worker threads
    NUM_WORKERS = 4  # Adjust based on CPU cores and I/O capacity

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_zip = {executor.submit(process_zip_file, zip_file): zip_file for zip_file in zip_files}
        for future in as_completed(future_to_zip):
            zip_file = future_to_zip[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Unhandled exception during processing of {zip_file.name}: {e}")

    logging.info("Data processing completed.")

if __name__ == "__main__":
    from concurrent.futures import ThreadPoolExecutor, as_completed
    main()
