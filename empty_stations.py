#!/usr/bin/env python3
import json
import time
from typing import Dict, Any

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
INPUT_TOPIC = "stations-status"   # événements de Q2
OUTPUT_TOPIC = "empty-stations"   # événements de Q3
GROUP_ID = "velib-empty-detector"


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer listening to stations-status."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[BROKER],
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer writing to empty-stations."""
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )


def get_available_bikes(station: Dict[str, Any]) -> int | None:
    """Extract available_bikes from station JSON."""
    val = station.get("available_bikes")
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def main() -> None:
    consumer = create_consumer()
    producer = create_producer()

    # station_number -> bool (True if station was empty at last state)
    last_empty_state: Dict[int | str, bool] = {}

    print(
        f"Listening to '{INPUT_TOPIC}' and sending empty/non-empty events to '{OUTPUT_TOPIC}'..."
    )

    for msg in consumer:
        station = msg.value

        station_number = station.get("number")
        if station_number is None:
            continue

        available_bikes = get_available_bikes(station)
        if available_bikes is None:
            continue

        is_empty_now = available_bikes == 0
        was_empty = last_empty_state.get(station_number)

        # First time we see this station: just record state, no event
        if was_empty is None:
            last_empty_state[station_number] = is_empty_now
            continue

        # Transitions
        became_empty = (was_empty is False) and is_empty_now
        became_non_empty = (was_empty is True) and (not is_empty_now)

        if became_empty:
            event = {
                "event_type": "BECAME_EMPTY",
                "number": station_number,
                "name": station.get("name"),
                "contract_name": station.get("contract_name"),
                "address": station.get("address"),
                "available_bikes": available_bikes,
                "available_stands": station.get("available_bike_stands"),
                "last_update": station.get("last_update"),
                "ts_app_ms": int(time.time() * 1000),
            }
            producer.send(OUTPUT_TOPIC, value=event)
            print(f"[EMPTY] Station {station_number} became empty")

        elif became_non_empty:
            event = {
                "event_type": "BECAME_NON_EMPTY",
                "number": station_number,
                "name": station.get("name"),
                "contract_name": station.get("contract_name"),
                "address": station.get("address"),
                "available_bikes": available_bikes,
                "available_stands": station.get("available_bike_stands"),
                "last_update": station.get("last_update"),
                "ts_app_ms": int(time.time() * 1000),
            }
            producer.send(OUTPUT_TOPIC, value=event)
            print(f"[FILLED] Station {station_number} is no longer empty")

        # Update last state
        last_empty_state[station_number] = is_empty_now


if __name__ == "__main__":
    main()
