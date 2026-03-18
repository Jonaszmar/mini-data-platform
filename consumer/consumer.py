import json
import os
import time
from confluent_kafka import Consumer, KafkaException


bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
group_id = os.getenv("KAFKA_GROUP_ID", "json-validator-group")

conf = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
}


def main():
    while True:
        try:
            consumer = Consumer(conf)
            break
        except Exception as e:
            print(f"Kafka client not ready yet: {e}")
            time.sleep(3)

    # REGEX subscription in librdkafka must start with ^
    consumer.subscribe(["^dbserver1\\.public\\..*"])
    print("Subscribed to topics matching regex: ^dbserver1\\.public\\..*")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                err_str = str(err)

                # Ignore startup phase when topics are not created yet
                if "Unknown topic or partition" in err_str:
                    print("Topics not created yet by Debezium. Waiting...")
                    time.sleep(3)
                    continue

                raise KafkaException(err)

            topic = msg.topic()
            raw = msg.value().decode("utf-8")

            try:
                data = json.loads(raw)
                print("=" * 80)
                print(f"TOPIC: {topic}")
                print(json.dumps(data, indent=2, ensure_ascii=False))
            except json.JSONDecodeError:
                print(f"Invalid JSON on topic {topic}: {raw}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()