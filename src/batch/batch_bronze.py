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

# Bronze is game grain: a game is only loaded once it's over. Anything else
# (FUT, PRE, LIVE, CRIT) would be stored half-finished and, because existing
# URLs are skipped, never refreshed. Live games are handled by the live pipeline.
FINAL_STATES = {"OFF", "FINAL"}

# One Delta commit per FLUSH_EVERY games instead of one per game. Each commit
# costs about a second on serverless, which used to be most of the run time.
# Raw JSON is up to a few hundred KB per game, so keep this modest.
FLUSH_EVERY = 100
MAX_FLUSH_RETRIES = 5
FLUSH_RETRY_BASE_DELAY = 30  # seconds; doubles each attempt (30, 60, 120, 240)

# Pause after every play-by-play request. The per-game commit used to add
# extra spacing between requests; without it the API may answer 429, so each
# 429 makes this pause longer for the rest of the run (never shorter).
REQUEST_PAUSE_S = 0.3
PAUSE_STEP_ON_429_S = 0.2
MAX_REQUEST_PAUSE_S = 3.0
request_pause = REQUEST_PAUSE_S

BRONZE_SCHEMA = "ingestion_timestamp TIMESTAMP, source STRING, raw_json STRING"

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
# Core functions – fetch a single game, write games in batches
# ----------------------------------------------------------------------
def fetch_game(game_id: str):
    """
    Fetch play-by-play JSON for a given game_id.
    Returns a Row for the bronze table, or None if the game couldn't be fetched
    or isn't final. A 429 is retried after a backoff and slows down every
    request after it.
    """
    global request_pause
    url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"
    logger.info(f"Fetching play-by-play for game_id={game_id} — {url}")

    for attempt in range(1, 6):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 429:
                wait = 2 ** attempt
                request_pause = min(request_pause + PAUSE_STEP_ON_429_S, MAX_REQUEST_PAUSE_S)
                logger.warning(f"429 for game {game_id}, waiting {wait}s (attempt {attempt}); "
                               f"pause between requests now {request_pause:.1f}s")
                time.sleep(wait)
                continue
            if response.status_code != 200:
                logger.error(f"Request failed with status {response.status_code} for game {game_id}")
                return None

            data = response.json()
            logger.info(f"Request succeeded — {len(response.content)} bytes received")

            # Safety net: the schedule said final, but don't trust it blindly
            game_state = data.get("gameState")
            if game_state not in FINAL_STATES:
                logger.info(f"Skipping game {game_id}: gameState={game_state} (not final yet)")
                return None

            return Row(
                ingestion_timestamp=datetime.now(),
                source=url,
                raw_json=json.dumps(data),
            )

        except Exception as e:
            logger.error(f"Exception while fetching game {game_id}: {e}")
            return None
    logger.error(f"Giving up on game {game_id} after repeated 429s")
    return None


def write_games(rows: list) -> None:
    """
    Append a batch of games to the bronze table in one commit, retrying on
    failure. If a commit actually landed but still reported an error, the
    retry stores those games twice; batch_silver keeps the newest row per game,
    so that's harmless.
    """
    for attempt in range(1, MAX_FLUSH_RETRIES + 1):
        try:
            spark.createDataFrame(rows, schema=BRONZE_SCHEMA) \
                .write \
                .format("delta") \
                .mode("append") \
                .saveAsTable("nhl.bronze.nhl_events")
            logger.info(f"Inserted {len(rows)} games into nhl.bronze.nhl_events")
            return
        except Exception as e:
            if attempt == MAX_FLUSH_RETRIES:
                raise
            delay = FLUSH_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"Write attempt {attempt}/{MAX_FLUSH_RETRIES} failed "
                f"({type(e).__name__}), retrying in {delay}s..."
            )
            time.sleep(delay)

# ----------------------------------------------------------------------
# Helpers to discover game IDs
# ----------------------------------------------------------------------
def get_game_ids_for_day(d: date) -> list:
    """
    Return game ID strings for finished games in the gameWeek starting at d.
    The schedule endpoint returns 7 days from d, so near today it includes
    future games — those are filtered out by FINAL_STATES.
    """
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
                    if game.get("gameState") in FINAL_STATES
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

    # ----- Process each new game, writing FLUSH_EVERY games per commit -----
    # Games still in the buffer when the run dies are lost from this run only:
    # they aren't in the table, so the next run fetches them again.
    buffer = []
    success_count = 0
    try:
        for idx, gid in enumerate(new_games, 1):
            logger.info(f"Processing {idx}/{len(new_games)}: game {gid}")
            row = fetch_game(gid)
            if row is not None:
                buffer.append(row)
            if len(buffer) >= FLUSH_EVERY:
                write_games(buffer)
                success_count += len(buffer)
                buffer = []
            # A small pause between requests to be kind to the API
            time.sleep(request_pause)
    finally:
        if buffer:
            write_games(buffer)
            success_count += len(buffer)

    logger.info(f"Done. Successfully stored {success_count}/{len(new_games)} new games "
                f"(pause between requests ended at {request_pause:.1f}s).")
