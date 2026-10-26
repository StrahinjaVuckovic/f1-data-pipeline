import os
import sys
import json
import time
import argparse
import requests
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from f1_data_pipeline.utils.logger import get_pipeline_logger

logger = get_pipeline_logger("f1_api_client")

BASE_URL = "https://api.jolpi.ca/ergast/f1"

session = requests.Session()
retries = Retry(
    total=5,                        
    backoff_factor=2,              
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False,          
    respect_retry_after_header=True 
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)


def fetch_f1_data(endpoint: str, limit: int = 100, offset: int = 0) -> Optional[List[Dict[str, Any]]]:
    """
    Generic method to fetch data from Ergast F1 API endpoints with automatic retries on 429 rate limits.
    Examples of endpoints: 'circuits', 'drivers', 'seasons', 'status', '2026/races'
    
    Returns:
        List[Dict]: Kada uspešno dohvati podatke (može biti i [] ako nema zapisa).
        None: Kada HTTP zahtev potpuno ne uspe nakon svih retry pokušaja.
    """
    clean_endpoint = endpoint.strip("/")
    url = f"{BASE_URL}/{clean_endpoint}/?limit={limit}&offset={offset}"

    logger.info(f"Fetching data from API: {url}")

    try:
        response = session.get(url, timeout=15)

        if response.status_code == 429:
            logger.error(f"Rate limit exceeded (429) for endpoint '{endpoint}' even after retries.")
            return None

        response.raise_for_status()
        data = response.json()

        mr_data = data.get("MRData", {})
        table_key = next((key for key in mr_data.keys() if key.endswith("Table")), None)

        if not table_key:
            logger.warning(f"No Table container found in API response for endpoint: {endpoint}")
            return []

        table_content = mr_data.get(table_key, {})
        list_key = next((key for key in table_content.keys() if isinstance(table_content[key], list)), None)

        if not list_key:
            logger.warning(f"No list data found inside {table_key}")
            return []

        results = table_content.get(list_key, [])
        logger.info(f"Successfully fetched {len(results)} records from endpoint '{endpoint}'")
        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from F1 API ({endpoint}): {e}")
        return None 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch and inspect data from Ergast F1 API")
    parser.add_argument("--endpoint", type=str, default="circuits", help="API endpoint (e.g. circuits, drivers, status, 2026/races)")
    parser.add_argument("--limit", type=int, default=5, help="Number of records to fetch for testing")
    
    args = parser.parse_args()

    print(f"\n--- Testing API Client: endpoint='{args.endpoint}', limit={args.limit} ---\n")
    records = fetch_f1_data(endpoint=args.endpoint, limit=args.limit)

    print("\n--- API RESPONSE DATA (Sample) ---")
    if records is not None:
        print(json.dumps(records, indent=2))
        print(f"\nTotal fetched records in sample: {len(records)}\n")
    else:
        print("\nFailed to fetch data due to API error/rate limit.\n")