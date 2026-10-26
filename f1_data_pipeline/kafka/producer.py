import os
import sys
import json
import time  
from confluent_kafka import Producer

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger
from f1_data_pipeline.utils.api_client import fetch_f1_data

logger = get_pipeline_logger("kafka_producer")

conf = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')}
producer = Producer(conf)
TOPIC_NAME = 'f1-api-data'


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_api_data_to_kafka(endpoint: str, limit: int = 1000) -> None:
    offset = 0
    total_sent = 0

    logger.info(f"Starting pagination for endpoint: '{endpoint}' with limit={limit}")

    while True:
        logger.info(f"Fetching data for '{endpoint}' (offset={offset})...")
        records = fetch_f1_data(endpoint=endpoint, limit=limit, offset=offset)

        if records is None:
            logger.warning(f"Failed to fetch data for '{endpoint}' (offset={offset}). Skipping this batch, but continuing pipeline.")
            break

        if len(records) == 0:
            logger.info(f"No more data found for '{endpoint}'. Pagination finished.")
            break

        for record in records:
            message_payload = {
                "api_endpoint": endpoint,
                "data": record
            }

            message_json = json.dumps(message_payload, default=str)
            record_key = str(record.get("circuitId", record.get("driverId", record.get("raceId", endpoint))))

            producer.produce(
                topic=TOPIC_NAME,
                key=record_key.encode('utf-8'),
                value=message_json.encode('utf-8'),
                callback=delivery_report
            )

            producer.poll(0)

        records_count = len(records)
        total_sent += records_count
        offset += limit

        logger.info(f"Successfully sent {records_count} records to Kafka. Total sent for '{endpoint}': {total_sent}")

        if records_count < limit:
            logger.info(f"Reached the last page for '{endpoint}'.")
            break


    producer.flush()
    logger.info(f"Completed '{endpoint}'. Total records sent: {total_sent}")


if __name__ == '__main__':
    logger.info("Starting Kafka API Producer (Full Historical Load)...")

    global_endpoints = [
        "circuits",
        "constructors",
        "drivers",
        "status",
        "races",    
        "results"    
    ]

    for ep in global_endpoints:
        produce_api_data_to_kafka(endpoint=ep, limit=1000)

    seasons_to_fetch = [2024, 2025, 2026]
    rounds_per_season = 24

    for season in seasons_to_fetch:
        for round_num in range(1, rounds_per_season + 1):
            laps_endpoint = f"{season}/{round_num}/laps"
            pitstops_endpoint = f"{season}/{round_num}/pitstops"

            produce_api_data_to_kafka(endpoint=laps_endpoint, limit=1000)

            produce_api_data_to_kafka(endpoint=pitstops_endpoint, limit=1000)


    logger.info("Kafka API Producer finished execution!")