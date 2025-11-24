#!/usr/bin/env python3
import time
import json
import requests
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC_NAME = "velib-stations"
POLL_INTERVAL = 10  # seconds

API_KEY = "f181de647beeff09ab27226e7169e95273dee1c0"
API_URL = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={API_KEY}"


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_stations() -> list:
    """Query the Bike Sharing System API and return the list of stations."""
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def main() -> None:
    producer = create_producer()
    print(f"Starting data ingestion to Kafka topic '{TOPIC_NAME}' every {POLL_INTERVAL} seconds...")

    while True:
        try:
            stations = fetch_stations()
            for station in stations:
                producer.send(TOPIC_NAME, station)
            producer.flush()
            print(f"Sent {len(stations)} station records to Kafka topic '{TOPIC_NAME}'")
        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
