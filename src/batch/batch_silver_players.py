"""
Silver layer — player dimension extracted from nhl.bronze.nhl_events.

Each game's rosterSpots array contains the identity of every player who
dressed for that game (name, position, sweater number, team, headshot URL).
This script parses those records and writes them to nhl.silver.nhl_players.

Schema: one row per (player_id, game_id) — the same player appears in
multiple rows across their career; use nhl.gold.dim_players for the
deduplicated, one-row-per-player version.

Idempotent: games already present in the silver player table are skipped.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, DateType, TimestampType,
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
# Schema
# ----------------------------------------------------------------------
PLAYER_SCHEMA = StructType([
    StructField("player_id",           IntegerType()),   # NHL player ID
    StructField("game_id",             LongType()),      # source game
    StructField("game_date",           DateType()),      # date of that game (for deduplication)
    StructField("season",              IntegerType()),   # season the game belongs to
    StructField("team_id",             IntegerType()),   # team the player dressed for
    StructField("first_name",          StringType()),
    StructField("last_name",           StringType()),
    StructField("full_name",           StringType()),
    StructField("position_code",       StringType()),    # L, R, C, D, G
    StructField("sweater_number",      IntegerType()),
    StructField("headshot_url",        StringType()),
    StructField("ingestion_timestamp", TimestampType()),
])


# ----------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------
def _str(v):
    return str(v) if v is not None else None

def _int(v):
    return int(v) if v is not None else None

def _date(v):
    if v is None:
        return None
    if isinstance(v, str):
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    return v

def _localized(field):
    """Extract the 'default' locale from an NHL API localized string object."""
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("default")
    return str(field)


def parse_roster_spots(data, ingestion_ts):
    """
    Yield one dict per player in rosterSpots for a single game JSON object.
    Returns an empty list if rosterSpots is absent (pre-API era games).
    """
    game_id   = data.get("id")
    season    = data.get("season")
    game_date = _date(data.get("gameDate"))
    rows = []

    for spot in data.get("rosterSpots") or []:
        player_id = _int(spot.get("playerId"))
        if not player_id:
            continue

        first = _localized(spot.get("firstName")) or ""
        last  = _localized(spot.get("lastName"))  or ""

        rows.append({
            "player_id":           player_id,
            "game_id":             _int(game_id),
            "game_date":           game_date,
            "season":              _int(season),
            "team_id":             _int(spot.get("teamId")),
            "first_name":          first,
            "last_name":           last,
            "full_name":           f"{first} {last}".strip() or None,
            "position_code":       _str(spot.get("positionCode")),
            "sweater_number":      _int(spot.get("sweaterNumber")),
            "headshot_url":        _str(spot.get("headshot")),
            "ingestion_timestamp": ingestion_ts,
        })

    return rows


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
try:
    existing_game_ids = {
        row.game_id
        for row in spark.table(SILVER_TABLE).select("game_id").distinct().collect()
    }
    logger.info(f"Found {len(existing_game_ids):,} already-processed game(s) in {SILVER_TABLE}.")
except Exception:
    existing_game_ids = set()
    logger.info(f"{SILVER_TABLE} does not exist yet — full load.")

bronze_rows = spark.table(BRONZE_TABLE).select("event_id", "raw_json", "ingestion_timestamp").collect()
logger.info(f"Bronze rows to scan: {len(bronze_rows):,}")

buf = []
processed = skipped = errors = 0
CHUNK_SIZE = 500

for row in bronze_rows:
    try:
        data    = json.loads(row.raw_json)
        game_id = data.get("id")
        if not game_id:
            continue

        if game_id in existing_game_ids:
            skipped += 1
            continue

        player_rows = parse_roster_spots(data, row.ingestion_timestamp)
        buf.extend(player_rows)
        existing_game_ids.add(game_id)
        processed += 1

        if processed % CHUNK_SIZE == 0:
            logger.info(f"Flushing chunk — {processed} games, {len(buf)} player rows…")
            spark.createDataFrame(buf, schema=PLAYER_SCHEMA) \
                .write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
            buf = []

    except Exception as e:
        logger.error(f"Failed on bronze row (event_id={row.event_id}): {e}")
        errors += 1

if buf:
    logger.info(f"Flushing final chunk — {len(buf)} player rows…")
    spark.createDataFrame(buf, schema=PLAYER_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(SILVER_TABLE)

logger.info(f"Done. Processed={processed} | Skipped={skipped} | Errors={errors}")
