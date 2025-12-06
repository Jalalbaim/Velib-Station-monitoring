#!/usr/bin/env python3
import time
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition

BROKER = "localhost:9092"
REFRESH_INTERVAL = 15  # seconds


def monitor_topics() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=BROKER,
        client_id="monitor-client",
    )
    consumer = KafkaConsumer(bootstrap_servers=[BROKER])

    print(f"Monitoring Kafka topics on broker '{BROKER}' (refresh every {REFRESH_INTERVAL}s)")

    try:
        while True:
            # refresh
            consumer.poll(timeout_ms=0)

            topics = sorted(admin.list_topics())

            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    continue

                for p in sorted(partitions):
                    tp = TopicPartition(topic, p)
                    # offset at the end of each partition
                    end_offsets = consumer.end_offsets([tp])
                    offset = end_offsets.get(tp, 0)
                    ts = time.strftime("%Y-%m-%d %H:%M:%S")

                    # Format: Topic-name, Partition-id, offset-id, timestamp
                    print(f"{topic},{p},{offset},{ts}")

            time.sleep(REFRESH_INTERVAL)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    finally:
        consumer.close()
        admin.close()


if __name__ == "__main__":
    monitor_topics()
