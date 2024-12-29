import os
import requests
import json
from datetime import datetime
import psycopg2
from psycopg2 import sql, extras
import sys

# Environment Variables for Configuration
STATION_INFO_URL = os.getenv("STATION_INFO_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_information.json")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "citibike_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "citibike_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "citibike_db")

# Initialize PostgreSQL Connection
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
    print("Connected to PostgreSQL successfully.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    sys.exit(1)

# Create Table if Not Exists
create_table_query = """
CREATE TABLE IF NOT EXISTS station_information (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(50) UNIQUE,
    name VARCHAR(255),
    short_name VARCHAR(50),
    lat FLOAT,
    lon FLOAT,
    region_id VARCHAR(50),
    capacity INTEGER,
    eightd_has_key_dispenser BOOLEAN,
    has_kiosk BOOLEAN,
    installed BOOLEAN,
    last_reported TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

try:
    cursor.execute(create_table_query)
    print("Ensured that station_information table exists.")
except Exception as e:
    print(f"Error creating table: {e}")
    conn.close()
    sys.exit(1)

def fetch_and_store_station_info():
    try:
        response = requests.get(STATION_INFO_URL)
        response.raise_for_status()
        data = response.json()
        stations = data.get('data', {}).get('stations', [])

        if not stations:
            print(f"[{datetime.now()}] No station data available to insert.")
            return

        # Prepare data for insertion
        insert_query = """
            INSERT INTO station_information (
                station_id,
                name,
                short_name,
                lat,
                lon,
                region_id,
                capacity,
                eightd_has_key_dispenser,
                has_kiosk,
                installed,
                last_reported
            ) VALUES %s
            ON CONFLICT (station_id) DO UPDATE SET
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                region_id = EXCLUDED.region_id,
                capacity = EXCLUDED.capacity,
                eightd_has_key_dispenser = EXCLUDED.eightd_has_key_dispenser,
                has_kiosk = EXCLUDED.has_kiosk,
                installed = EXCLUDED.installed,
                last_reported = EXCLUDED.last_reported,
                updated_at = CURRENT_TIMESTAMP;
        """

        records = []
        for station in stations:
            station_id = station.get('station_id')
            name = station.get('name')
            short_name = station.get('short_name')
            lat = station.get('lat')
            lon = station.get('lon')
            region_id = station.get('region_id')
            capacity = station.get('capacity')
            eightd_has_key_dispenser = station.get('eightd_has_key_dispenser', False)
            has_kiosk = station.get('has_kiosk', False)
            installed = station.get('installed', False)
            # Assuming last_reported is the current timestamp since station information might not have this field
            last_reported = datetime.now()

            records.append((
                station_id,
                name,
                short_name,
                lat,
                lon,
                region_id,
                capacity,
                eightd_has_key_dispenser,
                has_kiosk,
                installed,
                last_reported
            ))

        extras.execute_values(cursor, insert_query, records)
        print(f"[{datetime.now()}] Inserted/Updated {len(records)} records into PostgreSQL.")

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] Error fetching data: {e}")
    except psycopg2.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] Unexpected error: {e}")
    finally:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")

if __name__ == "__main__":
    fetch_and_store_station_info()
