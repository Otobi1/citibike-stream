import requests
import json
from datetime import datetime
from kafka import KafkaProducer

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
KAFKA_TOPIC = "station_status"

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_and_publish():
    try:
        response = requests.get(STATION_STATUS_URL)
        response.raise_for_status()
        data = response.json()
        for station in data['data']['stations']:
            producer.send(KAFKA_TOPIC, value=station)
        producer.flush()
        print(f"Data published at {datetime.now()}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except Exception as e:
        print(f"Error publishing data: {e}")

if __name__ == "__main__":
    fetch_and_publish()