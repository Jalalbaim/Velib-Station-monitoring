#!/usr/bin/env python3
import json
from kafka import KafkaConsumer

BROKER = "localhost:9092"
INPUT_TOPIC = "empty-stations"
GROUP_ID = "velib-alerts"


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer listening to empty-stations events."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def main() -> None:
    consumer = create_consumer()
    print(f"Listening for 'BECAME_EMPTY' events on '{INPUT_TOPIC}'...\n")

    for msg in consumer:
        event = msg.value
        event_type = event.get("event_type")

        # Only trigger alerts when a station becomes empty
        if event_type != "BECAME_EMPTY":
            continue

        address = event.get("address", "Unknown address")
        city = event.get("contract_name", "Unknown city")

        print("=============== A station just became EMPTY ===============")
        print(f"  Address : {address}")
        print(f"  City    : {city}")
        print(" ")


if __name__ == "__main__":
    main()
