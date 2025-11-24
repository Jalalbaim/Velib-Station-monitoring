#!/usr/bin/env python3
import json
from kafka import KafkaConsumer

BROKER = "localhost:9092"
INPUT_TOPIC = "empty-stations"
GROUP_ID = "velib-full-alerts"

# Consumer Kafka
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[BROKER],
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"🚴 Listening for full station alerts from topic '{INPUT_TOPIC}'...\n")

for message in consumer:
    event = message.value
    event_type = event.get("event_type")

    # On ne garde que les stations redevenues non vides
    if event_type == "BECAME_NON_EMPTY":
        name = event.get("name", "Unknown")
        address = event.get("address", "No address")
        city = event.get("contract_name", "Unknown")
        bikes = event.get("available_bikes", "?")
        print("✅ STATION RÉAPPROVISIONNÉE 🚲")
        print(f"  Nom : {name}")
        print(f"  Adresse : {address}")
        print(f"  Ville : {city}")
        print(f"  Vélos disponibles : {bikes}")
        print("-" * 40)
