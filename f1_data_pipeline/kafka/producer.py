import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import Optional
from confluent_kafka import Producer

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger
from f1_data_pipeline.utils.api_client import fetch_f1_data

logger = get_pipeline_logger("kafka_producer")

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
    'queue.buffering.max.messages': 100000
}
producer = Producer(conf)
TOPIC_NAME = 'f1-api-data'
PAGE_LIMIT = 1000
REQUEST_DELAY = 0.6 # bilo 25


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def extract_record_key(record: dict) -> Optional[str]:
    """Extract a logical partition key or return None for round-robin distribution."""
    for key_name in ["circuitId", "driverId", "constructorId", "statusId"]:
        if key_name in record:
            return str(record[key_name])

    if "driverId" in record and "lap" in record:
        return f"{record.get('driverId')}_{record.get('lap')}"
    if "driverId" in record:
        return str(record["driverId"])

    return None


def produce_api_data_to_kafka(endpoint: str) -> None:
    offset = 0
    total_sent = 0

    logger.info(f"Starting pagination for endpoint: '{endpoint}'")

    while True:
        api_result = fetch_f1_data(endpoint=endpoint, limit=PAGE_LIMIT, offset=offset)
        
        if isinstance(api_result, tuple):
            records, total_available = api_result
        else:
            records = api_result
            total_available = None

        if records is None:
            logger.warning(f"Failed to fetch data for '{endpoint}' (offset={offset}). Skipping.")
            break

        if not records:
            break

        for record in records:
            message_payload = {
                "api_endpoint": endpoint,
                "data": record
            }

            key_val = extract_record_key(record)
            key_bytes = key_val.encode('utf-8') if key_val else None

            while True:
                try:
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key_bytes,
                        value=json.dumps(message_payload, default=str).encode('utf-8'),
                        callback=delivery_report
                    )
                    break
                except BufferError:
                    logger.warning("Local Kafka queue full, waiting for flush...")
                    producer.poll(0.5)

            producer.poll(0)

        records_count = len(records)
        total_sent += records_count
        offset += PAGE_LIMIT

        if (total_available and offset >= total_available) or records_count < PAGE_LIMIT:
            break

        time.sleep(REQUEST_DELAY)

    producer.flush()
    logger.info(f"Completed '{endpoint}'. Total records sent: {total_sent}")


def process_seasons_dynamically(seasons: list) -> None:
    """Fetch season calendars and process only races that have actually taken place."""
    now_utc = datetime.now(timezone.utc)

    for season in seasons:
        logger.info(f"Fetching race calendar for season {season}...")
        api_result = fetch_f1_data(f"{season}/races", limit=100)
        races = api_result[0] if isinstance(api_result, tuple) else api_result

        if not races:
            logger.warning(f"No race schedule found for season {season}.")
            continue

        for race in races:
            round_num = race.get("round")
            race_date_str = race.get("date")

            if race_date_str:
                try:
                    race_date = datetime.strptime(race_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if race_date > now_utc:
                        logger.info(f"Race {season} Round {round_num} has not occurred yet ({race_date_str}). Skipping.")
                        continue
                except ValueError:
                    pass

            logger.info(f"Processing season {season}, round {round_num} ({race.get('raceName')})...")
            
            produce_api_data_to_kafka(f"{season}/{round_num}/results")
            time.sleep(REQUEST_DELAY)

            produce_api_data_to_kafka(f"{season}/{round_num}/pitstops")
            time.sleep(REQUEST_DELAY)

            produce_api_data_to_kafka(f"{season}/{round_num}/laps")
            time.sleep(REQUEST_DELAY)


if __name__ == '__main__':
    logger.info("Starting Kafka API Producer...")

    global_endpoints = [
        "circuits",
        "constructors",
        "drivers",
        "status",
        "races"
    ]

    for ep in global_endpoints:
        produce_api_data_to_kafka(endpoint=ep)
        time.sleep(REQUEST_DELAY)

    seasons_to_fetch = [2024, 2025, 2026]
    process_seasons_dynamically(seasons_to_fetch)

    logger.info("Kafka API Producer completed successfully!")