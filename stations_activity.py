#!/usr/bin/env python3
import json
from typing import Dict, Any
from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
INPUT_TOPIC = "velib-stations"
OUTPUT_TOPIC = "stations-status"


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer listening to velib-stations."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def has_status_changed(
    prev: Dict[str, Any],
    available_bikes: int,
    available_stands: int,
) -> bool:
    """Return True if bikes or free slots have changed compared to previous state."""
    return (
        prev is None
        or prev.get("available_bikes") != available_bikes
        or prev.get("available_bike_stands") != available_stands
    )


def main() -> None:
    consumer = create_consumer()
    producer = create_producer()

    # Store previous status per station number
    previous_status: Dict[int, Dict[str, int]] = {}

    print(
        f"Listening to '{INPUT_TOPIC}' and forwarding status changes to '{OUTPUT_TOPIC}'..."
    )

    for message in consumer:
        station = message.value

        station_number = station.get("number")
        available_bikes = station.get("available_bikes")
        available_stands = station.get("available_bike_stands")

        # Skip malformed messages
        if station_number is None:
            continue

        prev = previous_status.get(station_number)

        if has_status_changed(prev, available_bikes, available_stands):
            # Send full station details to the output topic
            producer.send(OUTPUT_TOPIC, station)
            producer.flush()

            previous_status[station_number] = {
                "available_bikes": available_bikes,
                "available_bike_stands": available_stands,
            }

            print(
                f"Station {station_number} status changed: "
                f"bikes={available_bikes}, free_slots={available_stands} -> sent to '{OUTPUT_TOPIC}'"
            )


if __name__ == "__main__":
    main()
