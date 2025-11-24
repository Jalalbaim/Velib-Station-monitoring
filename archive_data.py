#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer

BROKER = "localhost:9092"
INPUT_TOPIC = "velib-stations"
OUTPUT_FILE = "velib_archive.txt"
GROUP_ID = "velib-archiver"


def create_consumer() -> KafkaConsumer:
    """Create and return a Kafka consumer for archiving."""
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

    print(f"Archiving data from '{INPUT_TOPIC}' into '{OUTPUT_FILE}'...\n")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
        counter = 0

        for msg in consumer:
            station = msg.value

            # Write station JSON on a single line
            file.write(json.dumps(station, ensure_ascii=False) + "\n")
            counter += 1

            # Regular flush to avoid data loss
            if counter % 100 == 0:
                file.flush()
                print(f"[{time.strftime('%H:%M:%S')}] Archived {counter} entries...")


if __name__ == "__main__":
    main()
