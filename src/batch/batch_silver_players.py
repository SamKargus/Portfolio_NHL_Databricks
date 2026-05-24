"""
Silver layer — deduplicated player lookup extracted from nhl.bronze.nhl_events.

Reads rosterSpots from every game in bronze, keeps the most recent record
per player_id (by game date), and writes one row per player to
nhl.silver.nhl_players.  Fully overwritten on each run.

Downstream: nhl.gold.dim_players reads directly from this table.
"""

import json
import logging
from pathlib import Path
from datetime import datetime, date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType, TimestampType,
)

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"batch_silver_players_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("batch_silver_players")
logger.info(f"batch_silver_players started — log: {log_file}")

BRONZE_TABLE = "nhl.bronze.nhl_events"
SILVER_TABLE  = "nhl.silver.nhl_players"

# ----------------------------------------------------------------------
# Schema — one row per player_id
# ----------------------------------------------------------------------
PLAYER_SCHEMA = StructType([
    StructField("player_id",            IntegerType()),
    StructField("first_name",           StringType()),
    StructField("last_name",            StringType()),
    StructField("full_name",            StringType()),
    StructField("position_code",        StringType()),   # L, R, C, D, G
    StructField("sweater_number",       IntegerType()),  # most recent known
    StructField("team_id",              IntegerType()),  # most recent known
    StructField("headshot_url",         StringType()),
    StructField("last_seen_game_date",  DateType()),
    StructField("ingestion_timestamp",  TimestampType()),
])


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _int(v):
    return int(v) if v is not None else None

def _str(v):
    return str(v) if v is not None else None

def _date(v):
    if v is None:
        return None
    if isinstance(v, str):
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    return v

def _localized(field):
    """Extract the 'default' locale from an NHL API localised string object."""
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("default")
    return str(field)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
bronze_rows = spark.table(BRONZE_TABLE).select("raw_json").collect()
logger.info(f"Scanning {len(bronze_rows):,} bronze rows for player roster spots…")

# Build a dict keyed by player_id; keep whichever record has the most
# recent game_date so sweater number and team reflect the latest known state.
players: dict[int, dict] = {}
ingestion_ts = datetime.utcnow()

for row in bronze_rows:
    try:
        data      = json.loads(row.raw_json)
        game_date = _date(data.get("gameDate"))

        for spot in data.get("rosterSpots") or []:
            player_id = _int(spot.get("playerId"))
            if not player_id:
                continue

            existing = players.get(player_id)
            # Replace only if this game is more recent than what we already have
            if existing and game_date and existing["last_seen_game_date"]:
                if game_date <= existing["last_seen_game_date"]:
                    continue

            first = _localized(spot.get("firstName")) or ""
            last  = _localized(spot.get("lastName"))  or ""

            players[player_id] = {
                "player_id":           player_id,
                "first_name":          first,
                "last_name":           last,
                "full_name":           f"{first} {last}".strip() or None,
                "position_code":       _str(spot.get("positionCode")),
                "sweater_number":      _int(spot.get("sweaterNumber")),
                "team_id":             _int(spot.get("teamId")),
                "headshot_url":        _str(spot.get("headshot")),
                "last_seen_game_date": game_date,
                "ingestion_timestamp": ingestion_ts,
            }
    except Exception as e:
        logger.error(f"Error parsing roster spots: {e}")

logger.info(f"Discovered {len(players):,} unique players.")

(
    spark.createDataFrame(list(players.values()), schema=PLAYER_SCHEMA)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
logger.info(f"Written: {SILVER_TABLE}")
