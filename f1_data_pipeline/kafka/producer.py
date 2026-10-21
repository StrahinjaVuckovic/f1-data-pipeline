import os
import sys
import time
import json
import pandas as pd
from confluent_kafka import Producer

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger

logger = get_pipeline_logger("kafka_producer")

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC_NAME = 'f1-raw-telemetry'


def delivery_report(err, msg):
    """Callback triggered on successful or failed message delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def run_producer(file_path: str = None, limit_rows: int = 20) -> None:
    if file_path is None:
        file_path = os.path.join(ROOT_DIR, "data", "dataEngineeringDataset.csv")

    if not os.path.exists(file_path):
        logger.error(f"File not found at path: {file_path}")
        return

    logger.info(f"Loading CSV file from: {file_path}")
    df = pd.read_csv(file_path, nrows=limit_rows)

    logger.info(f"Starting message streaming to topic '{TOPIC_NAME}'...")

    for index, row in df.iterrows():
        message_data = row.to_dict()
        message_json = json.dumps(message_data, default=str)

        producer.produce(
            topic=TOPIC_NAME,
            key=str(index),
            value=message_json.encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        logger.info(f"Simulating live telemetry: Sent row #{index + 1}")
        time.sleep(1)  

    producer.flush()
    logger.info("All test messages successfully sent to Kafka!")


if __name__ == '__main__':
    run_producer()