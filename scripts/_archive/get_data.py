
import requests
import json
from datetime import datetime

STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

def fetch_station_status() -> None:
    try:
        response = requests.get(STATION_STATUS_URL)
        response.raise_for_status()
        data = response.json()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        with open(f"../data/station_status_{timestamp}.json", 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Data retrieved and saved at {timestamp}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except OSError as e:
        print(f"Error saving data: {e}")

if __name__ == "__main__":
    fetch_station_status()