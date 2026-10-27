import os
import sys
import argparse
import requests
import time
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger

logger = get_pipeline_logger("f1_api_client")

BASE_URL = "https://api.jolpi.ca/ergast/f1"
REQUEST_DELAY = 1

session = requests.Session()
retries = Retry(
    total=5, # bilo 3
    backoff_factor=2, # bilo 1
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False,
    respect_retry_after_header=True
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)


def _extract_records_from_response(mr_data: Dict[str, Any]) -> List[Dict[str, Any]]:
  
    table_key = next((k for k in mr_data.keys() if k.endswith("Table")), None)
    if not table_key:
        return []

    table_content = mr_data.get(table_key, {})

    if "Races" in table_content:
        return table_content.get("Races", [])

    list_key = next((k for k in table_content.keys() if isinstance(table_content[k], list)), None)
    if list_key:
        return table_content.get(list_key, [])

    return []


def fetch_f1_data(endpoint: str, limit: int = 1000, offset: int = 0) -> Optional[List[Dict[str, Any]]]:
  
    clean_endpoint = endpoint.strip("/")
    url = f"{BASE_URL}/{clean_endpoint}/?limit={limit}&offset={offset}"

    logger.debug(f"Fetching: {url}")

    try:
        response = session.get(url, timeout=15)

        if response.status_code == 429:
            logger.error(f"Rate limit exceeded (429) for '{endpoint}' after all retry attempts.")
            return None

        response.raise_for_status()
        data = response.json()

        mr_data = data.get("MRData", {})
        records = _extract_records_from_response(mr_data)

        logger.info(f"Successfully fetched {len(records)} records from endpoint '{endpoint}'")

        time.sleep(REQUEST_DELAY)

        return records

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API ({endpoint}): {e}")
        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch and inspect data from Ergast/Jolpica F1 API")
    parser.add_argument("--endpoint", type=str, default="circuits", help="API endpoint")
    parser.add_argument("--limit", type=int, default=5, help="Number of records to fetch")

    args = parser.parse_args()
    records = fetch_f1_data(endpoint=args.endpoint, limit=args.limit)

    if records is not None:
        print(f"Sample fetched ({len(records)} records):")
        import json
        print(json.dumps(records, indent=2))
    else:
        print("Failed to fetch data.")