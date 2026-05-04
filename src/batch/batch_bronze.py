import requests
import logging
import os
from pathlib import Path
from datetime import datetime
import json
from pyspark.sql import Row

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"batch_bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("batch_bronze")
logger.info(f"batch_bronze started — log: {log_file}")

BASE_URL = "https://api-web.nhle.com/v1"

# Ensure the target Delta table exists (idempotent)
spark.sql("""
    CREATE TABLE IF NOT EXISTS nhl.bronze.nhl_events (
        event_id BIGINT GENERATED ALWAYS AS IDENTITY,
        ingestion_timestamp TIMESTAMP,
        source STRING,
        raw_json STRING
    ) USING DELTA
""")
logger.info("Table nhl.bronze.nhl_events is ready")

# ----------------------------------------------------------------------
# Core function – call this for each game
# ----------------------------------------------------------------------
def process_game(game_id: str):
    """
    Fetch play-by-play JSON for a given game_id and append it to the bronze Delta table.
    Returns True if the game was successfully stored, False otherwise.
    """
    url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"
    logger.info(f"Fetching play-by-play for game_id={game_id} — {url}")

    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.error(f"Request failed with status {response.status_code} for game {game_id}")
            return False

        data = response.json()
        logger.info(f"Request succeeded — {len(response.content)} bytes received")

        raw_json_str = json.dumps(data)

        row = Row(
            ingestion_timestamp=datetime.now(),
            source=url,          # or use f"play-by-play/{game_id}"
            raw_json=raw_json_str
        )
        df = spark.createDataFrame([row])

        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("nhl.bronze.nhl_events")

        logger.info(f"Data for game {game_id} inserted into nhl.bronze.nhl_events")
        return True

    except Exception as e:
        logger.error(f"Exception while processing game {game_id}: {e}")
        return False
    

# ----------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Process a single game
    process_game("1917030215")

    # Or loop over a list of IDs
    # for gid in ["2023020001", "2023020002"]:
    #     process_game(gid)