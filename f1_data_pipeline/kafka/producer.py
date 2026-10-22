import os
import sys
import json
from confluent_kafka import Producer

# Set paths to the project root directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger
from f1_data_pipeline.utils.api_client import fetch_f1_data

logger = get_pipeline_logger("kafka_producer")

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
TOPIC_NAME = 'f1-api-data'


def delivery_report(err, msg):
    """Callback triggered on successful or failed message delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_api_data_to_kafka(endpoint: str, limit: int = 100) -> None:
    """Fetches ALL data from the API using pagination and sends it to Kafka."""
    offset = 0
    total_sent = 0
    
    logger.info(f"Starting pagination for endpoint: '{endpoint}' with limit={limit}")

    while True:
        logger.info(f"Fetching data for '{endpoint}' (offset={offset})...")
        records = fetch_f1_data(endpoint=endpoint, limit=limit, offset=offset)

        if not records:
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
   
    endpoints_to_ingest = [
        "circuits", 
        "constructors",
        "drivers", 
        "status", 
        "2026/races",
        "2026/1/results",  
        "2026/1/laps",     
        "2026/1/pitstops"  
    ]
    
    logger.info("Starting Kafka API Producer (Full Pagination)...")
    
    for ep in endpoints_to_ingest:
        produce_api_data_to_kafka(endpoint=ep, limit=50)
        
    logger.info("Kafka API Producer finished execution!")