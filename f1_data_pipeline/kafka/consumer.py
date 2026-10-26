import os
import sys
import json
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import text

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger
from f1_data_pipeline.bronze_ingestion import get_engine

logger = get_pipeline_logger("kafka_consumer")

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
    'group.id': 'f1_bronze_consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
TOPIC_NAME = 'f1-api-data'

def insert_into_bronze_api(engine, endpoint: str, api_payload: dict) -> None:
    """Inserts a single API record into the bronze_api.raw_data table."""
    query = text("""
        INSERT INTO bronze_api.raw_data (endpoint, api_payload)
        VALUES (:endpoint, :api_payload)
    """)
    
    payload_json = json.dumps(api_payload)
    
    with engine.begin() as connection:
        connection.execute(query, {"endpoint": endpoint, "api_payload": payload_json})

def consume_api_data(empty_polls_limit: int = 3) -> None:
    """Main loop that polls the Kafka topic and writes data to the database."""
    logger.info("Connecting to the database...")
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return

    consumer.subscribe([TOPIC_NAME])
    logger.info(f"Consumer successfully subscribed to topic: '{TOPIC_NAME}'. Waiting for messages...")

    empty_polls = 0
    total_consumed = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                logger.info(f"No new messages received ({empty_polls}/{empty_polls_limit})...")
                
                if empty_polls >= empty_polls_limit:
                    logger.info("All messages consumed. Consumer shutting down successfully.")
                    break
                continue

            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break

            try:
                message_value = json.loads(msg.value().decode('utf-8'))
                endpoint = message_value.get("api_endpoint")
                data = message_value.get("data")
                
                if endpoint and data:
                    insert_into_bronze_api(engine, endpoint, data)
                    total_consumed += 1
                    logger.debug(f"Successfully inserted record for endpoint: {endpoint}")
                else:
                    logger.warning("Invalid message format (missing endpoint or data).")

            except json.JSONDecodeError:
                logger.error("Error decoding JSON message from Kafka.")
            except Exception as db_err:
                logger.error(f"Database insertion error: {db_err}")

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        logger.info(f"Kafka Consumer shut down.")

if __name__ == '__main__':
    consume_api_data()