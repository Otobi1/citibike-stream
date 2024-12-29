
import os
import requests
from time import sleep
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Configuration
TRIPDATA_BASE_URL = "https://s3.amazonaws.com/tripdata/"
DOWNLOAD_DIR = os.path.expanduser("./data/downloaded_tripdata")
MAX_RETRIES = 5
BACKOFF_FACTOR = 3

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logging.info(f"Download directory set to: {DOWNLOAD_DIR}")


def get_available_files():
    try:
        response = requests.get(TRIPDATA_BASE_URL)
        response.raise_for_status()

        # Parse the XML response
        root = ET.fromstring(response.content)
        namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        keys = [content.find('s3:Key', namespace).text for content in root.findall('s3:Contents', namespace)]

        # Combined regex for yearly and monthly files with and without .csv
        pattern = re.compile(r'^(\d{4}-citibike-tripdata\.zip|\d{6}-citibike-tripdata(?:\.csv)?\.zip)$')
        tripdata_files = [key for key in keys if pattern.match(key)]

        logging.info(f"Found {len(tripdata_files)} tripdata files matching the pattern.")
        return tripdata_files
    except Exception as e:
        logging.error(f"Failed to fetch available files: {e}")
        return []


def is_file_complete(file_path, min_size=1000):
    """
    Check if the file exists and its size is greater than a minimum threshold.
    Args:
        file_path (str): Path to the file.
        min_size (int): Minimum file size in bytes.
    Returns:
        bool: True if file exists and size >= min_size, else False.
    """
    if not os.path.exists(file_path):
        return False
    if os.path.getsize(file_path) < min_size:
        logging.warning(f"File {file_path} exists but is smaller than expected ({os.path.getsize(file_path)} bytes).")
        return False
    return True


def download_file(file_name):
    file_url = f"{TRIPDATA_BASE_URL}{file_name}"
    local_path = os.path.join(DOWNLOAD_DIR, file_name)

    if is_file_complete(local_path):
        logging.info(f"File already exists and is complete: {file_name}. Skipping download.")
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Downloading {file_url} (Attempt {attempt})")
            with requests.get(file_url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # Filter out keep-alive chunks
                            f.write(chunk)
            logging.info(f"Successfully downloaded {file_name}")
            return
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt} failed for {file_name}: {e}")
            if attempt == MAX_RETRIES:
                logging.error(f"Failed to download {file_name} after {MAX_RETRIES} attempts.")
                # Optionally, remove incomplete file
                if os.path.exists(local_path):
                    os.remove(local_path)
                    logging.info(f"Removed incomplete file: {file_name}")
            else:
                sleep_time = BACKOFF_FACTOR ** attempt
                logging.info(f"Retrying in {sleep_time} seconds...")
                sleep(sleep_time)


def main():
    tripdata_files = get_available_files()
    if not tripdata_files:
        logging.warning("No tripdata files found to download.")
        return
    for file_name in tripdata_files:
        download_file(file_name)
    logging.info("Download process completed.")


if __name__ == "__main__":
    main()
