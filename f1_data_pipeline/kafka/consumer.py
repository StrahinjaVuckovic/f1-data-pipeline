import os
import sys
import json
from confluent_kafka import Consumer, KafkaError

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger

logger = get_pipeline_logger("kafka_consumer")

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'f1-bronze-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
TOPIC_NAME = 'f1-raw-telemetry'


def run_consumer(max_idle_seconds: int = 5) -> None:
    consumer.subscribe([TOPIC_NAME])
    logger.info(f"Consumer started! Listening to topic: '{TOPIC_NAME}'... (Timeout set to {max_idle_seconds}s)")

    idle_counter = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                idle_counter += 1
                if idle_counter >= max_idle_seconds:
                    logger.info(f"No new messages for {max_idle_seconds} seconds. Stopping consumer automatically.")
                    break
                continue

            idle_counter = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            value_str = msg.value().decode('utf-8')
            data = json.loads(value_str)

            logger.info(f"[RECEIVED FROM KAFKA] RaceID: {data.get('raceId')} | DriverID: {data.get('driverId')} | Year: {data.get('year')}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer manually...")
    finally:
        consumer.close()


if __name__ == '__main__':
    run_consumer()