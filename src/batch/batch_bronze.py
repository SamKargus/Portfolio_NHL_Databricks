import requests
import logging
import os
from pathlib import Path
from datetime import datetime, date, timedelta
import time
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

# Ensure the target Delta table exists
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
# Core function – fetch & store a single game
# ----------------------------------------------------------------------
def process_game(game_id: str) -> bool:
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
            source=url,
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
# Helpers to discover game IDs
# ----------------------------------------------------------------------
def get_game_ids_for_day(d: date) -> list:
    """Return list of game ID strings for a single day."""
    url = f"{BASE_URL}/schedule/{d.isoformat()}"
    for attempt in range(1, 6):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return [
                    str(game["id"])
                    for gw in data.get("gameWeek", [])
                    for game in gw.get("games", [])
                ]
            elif resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning(f"429 for {d}, waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
            else:
                return []
        except Exception as e:
            logger.warning(f"Error fetching {d}: {e}, waiting before retry")
            time.sleep(2 ** attempt)
    return []

def get_all_game_ids(start: date, end: date) -> list:
    """Collect all game IDs between start and end (inclusive), stepping weekly."""
    ids = []
    current = start
    while current <= end:
        week_ids = get_game_ids_for_day(current)
        ids.extend(week_ids)
        logger.info(f"Week of {current} → {len(week_ids)} games")
        current += timedelta(weeks=1)
        time.sleep(0.3)
    return ids

# ----------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # ----- Set your desired date range -----
    START_DATE = date(1916, 1, 1)
    END_DATE   = date.today()

    logger.info(f"Collecting game IDs from {START_DATE} to {END_DATE}")
    all_games = list(dict.fromkeys(get_all_game_ids(START_DATE, END_DATE)))
    logger.info(f"Total unique game IDs found: {len(all_games)}")

    # ----- Skip games already in the bronze table -----
    existing_df = spark.sql("SELECT DISTINCT source FROM nhl.bronze.nhl_events")
    # Assumes source stores the full URL (as used in process_game).
    existing_urls = {r.source for r in existing_df.collect() if r.source}
    new_games = [gid for gid in all_games
                 if f"{BASE_URL}/gamecenter/{gid}/play-by-play" not in existing_urls]
    logger.info(f"Already loaded: {len(existing_urls)} | New to process: {len(new_games)}")

    # ----- Process each new game -----
    success_count = 0
    for idx, gid in enumerate(new_games, 1):
        logger.info(f"Processing {idx}/{len(new_games)}: game {gid}")
        if process_game(gid):
            success_count += 1
        # A small pause between requests to be kind to the API
        time.sleep(0.3)

    logger.info(f"Done. Successfully stored {success_count}/{len(new_games)} new games.")