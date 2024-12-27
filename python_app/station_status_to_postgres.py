import os
import requests
import json
from datetime import datetime
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql, extras
import time

# Environment Variables for Configuration
STATION_STATUS_URL = os.getenv("STATION_STATUS_URL", "https://gbfs.citibikenyc.com/gbfs/en/station_status.json")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "station_status")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "citibike_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "citibike_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "citibike_db")

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
    exit(1)

# Create Table if Not Exists
create_table_query = """
CREATE TABLE IF NOT EXISTS station_status (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(50),
    num_bikes_available INTEGER,
    num_docks_available INTEGER,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    last_reported TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_station_id ON station_status(station_id);
"""

try:
    cursor.execute(create_table_query)
    print("Ensured that station_status table exists with history tracking.")
except Exception as e:
    print(f"Error creating table: {e}")
    exit(1)

def fetch_and_store():
    try:
        response = requests.get(STATION_STATUS_URL)
        response.raise_for_status()
        data = response.json()
        stations = data.get('data', {}).get('stations', [])

        # Prepare data for insertion
        insert_query = """
                INSERT INTO station_status (
                    station_id,
                    num_bikes_available,
                    num_docks_available,
                    is_installed,
                    is_renting,
                    is_returning,
                    last_reported
                ) VALUES %s
                """

        records = []
        for station in stations:
            station_id = station.get('station_id')
            num_bikes_available = station.get('num_bikes_available')
            num_docks_available = station.get('num_docks_available')
            is_installed = bool(station.get('is_installed'))
            is_renting = bool(station.get('is_renting'))
            is_returning = bool(station.get('is_returning'))
            last_reported = datetime.fromtimestamp(station.get('last_reported'))

            records.append((
                station_id,
                num_bikes_available,
                num_docks_available,
                is_installed,
                is_renting,
                is_returning,
                last_reported
            ))

            # Publish to Kafka
            producer.send(KAFKA_TOPIC, value=station)

        if records:
            extras.execute_values(cursor, insert_query, records)
            print(f"[{datetime.now()}] Inserted/Updated {len(records)} records into PostgreSQL.")
        else:
            print(f"[{datetime.now()}] No station data available to insert.")

        producer.flush()
        print(f"[{datetime.now()}] Data published to Kafka.")

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] Error fetching data: {e}")
    except psycopg2.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] Unexpected error: {e}")

if __name__ == "__main__":
    while True:
        fetch_and_store()
        time.sleep(60)  # Fetch and store data every 60 seconds (could also be every 5 minutes)
