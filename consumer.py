import time
from kafka import KafkaConsumer

topic_name = "topic1"  # même nom que celui du producer

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers="localhost:9092",
    group_id="app1",              # guillemets droits
    auto_offset_reset="earliest"  # lit depuis le début
)

for message in consumer:
    print(
        "Received message: {} from topic: {}, partition: {}, offset: {}".format(
            message.value.decode("utf-8"),  # décodage texte
            message.topic,
            message.partition,
            message.offset
        )
    )
    time.sleep(1)

