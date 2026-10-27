import os
import sys
import json
import argparse
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

    producer.flush()
    logger.info(f"Completed '{endpoint}'. Total records sent: {total_sent}")


def process_seasons_dynamically(seasons: list, start_season: int, start_round: int) -> None:
    """Fetch season calendars and process only races that have actually taken place."""
    now_utc = datetime.now(timezone.utc)

    for season in seasons:
        # 1. Preskoči celu sezonu ako je starija od start_season
        if season < start_season:
            logger.info(f"Skipping season {season} (prior to start_season {start_season}).")
            continue

        logger.info(f"Fetching race calendar for season {season}...")
        api_result = fetch_f1_data(f"{season}/races", limit=100)
        races = api_result[0] if isinstance(api_result, tuple) else api_result

        if not races:
            logger.warning(f"No race schedule found for season {season}.")
            continue

        for race in races:
            round_num_str = race.get("round")
            if not round_num_str:
                continue
                
            round_num = int(round_num_str)
            race_date_str = race.get("date")

            # 2. Za start_season preskoči sve runde koje su manje ili jednake start_round
            if season == start_season and round_num <= start_round:
                logger.info(f"Skipping season {season} round {round_num} (already processed in database).")
                continue

            if race_date_str:
                try:
                    race_date = datetime.strptime(race_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if race_date > now_utc:
                        logger.info(f"Race {season} Round {round_num} has not occurred yet ({race_date_str}). Stopping processing for this season.")
                        break 
                except ValueError:
                    pass

            logger.info(f"Processing season {season}, round {round_num} ({race.get('raceName')})...")
            
            produce_api_data_to_kafka(f"{season}/{round_num}/results")
            produce_api_data_to_kafka(f"{season}/{round_num}/pitstops")
            produce_api_data_to_kafka(f"{season}/{round_num}/laps")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="F1 API Kafka Producer")
    parser.add_argument('--start-season', type=int, default=2024, help="Season to start fetching from")
    parser.add_argument('--start-round', type=int, default=0, help="Last processed round to skip")
    parser.add_argument('--max-season', type=int, default=None, help="Maximum season to fetch (inclusive)")
    parser.add_argument('--update-globals', action='store_true', help="Fetch global dimensions (drivers, circuits, etc.)")
    args = parser.parse_args()

    logger.info(f"Starting Kafka API Producer (Incremental from Season {args.start_season}, Round {args.start_round})...")

    if args.update_globals:
        logger.info("Flag --update-globals detected. Fetching global dimensions...")
        global_endpoints = ["circuits", "constructors", "drivers", "status"]
        for ep in global_endpoints:
            produce_api_data_to_kafka(endpoint=ep)
    else:
        logger.info("Skipping global dimensions. Use --update-globals to fetch them.")

    current_year = datetime.now(timezone.utc).year
    end_season = args.max_season if args.max_season is not None else current_year

    seasons_to_fetch = list(range(args.start_season, end_season + 1))
    logger.info(f"Seasons scheduled for processing: {seasons_to_fetch}")

    process_seasons_dynamically(seasons_to_fetch, args.start_season, args.start_round)

    logger.info("Kafka API Producer completed successfully!")