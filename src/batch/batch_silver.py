import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampType

# Shared silver parsing lives in src/common (the live pipeline uses it too)
_cwd = Path.cwd()
sys.path.insert(0, str(next(
    (p / "src" / "common" for p in [_cwd, *_cwd.parents] if (p / "src" / "common").is_dir()),
    _cwd,
)))
from silver_parse import (  # noqa: E402
    GAMES_TABLE, ROSTERS_TABLE, PLAYS_TABLE,
    GAMES_SCHEMA, ROSTERS_SCHEMA, PLAYS_SCHEMA,
    parse_game, parse_roster, parse_plays,
)

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"batch_silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("batch_silver")
logger.info(f"batch_silver started — log: {log_file}")

CHUNK_SIZE = 500         # flush silver rows to Delta every N games
BRONZE_BATCH_SIZE = 50  # raw JSON rows fetched from bronze per batch
MAX_FLUSH_RETRIES = 5
FLUSH_RETRY_BASE_DELAY = 30  # seconds; doubles each attempt (30, 60, 120, 240, ...)

RUN_TIMESTAMP = datetime.now()  # when this run wrote to silver (ingestion_timestamp)


# One row per bronze row this script has handled, so a run only parses bronze
# rows it hasn't seen. Drop it together with the silver tables when rebuilding.
PROCESSED_TABLE = "nhl.silver.processed_games"


def _with_retries(write, what: str) -> None:
    for attempt in range(1, MAX_FLUSH_RETRIES + 1):
        try:
            write()
            return
        except Exception as e:
            if attempt == MAX_FLUSH_RETRIES:
                raise
            delay = FLUSH_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"{what} attempt {attempt}/{MAX_FLUSH_RETRIES} failed "
                f"({type(e).__name__}), retrying in {delay}s..."
            )
            time.sleep(delay)


def _replace_games(df_rows: list, schema, table: str, predicate: str) -> None:
    _with_retries(
        lambda: spark.createDataFrame(df_rows, schema=schema)
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", predicate)
            .saveAsTable(table),
        f"Write to {table}",
    )


def _flush(games: list, rosters: list, plays: list, tracking: list) -> None:
    # replaceWhere swaps out every existing row for these games (e.g. rows the
    # live pipeline wrote while the game was on) in one atomic commit per
    # table, so a retry can never duplicate rows. Delta has no transaction
    # across tables, so games goes last: a game row means its rosters and
    # plays are already in place.
    if games:  # empty when the chunk only held duplicate bronze rows
        predicate = f"game_id IN ({','.join(str(g['game_id']) for g in games)})"
        _replace_games(rosters, ROSTERS_SCHEMA, ROSTERS_TABLE, predicate)
        _replace_games(plays,   PLAYS_SCHEMA,   PLAYS_TABLE,   predicate)
        _replace_games(games,   GAMES_SCHEMA,   GAMES_TABLE,   predicate)
    # Recorded only after silver is written: if the run dies in between, the
    # chunk is simply re-processed next run (replaceWhere is idempotent).
    _with_retries(
        lambda: spark.createDataFrame(tracking, schema=PROCESSED_SCHEMA)
            .write.format("delta").mode("append")
            .saveAsTable(PROCESSED_TABLE),
        "Tracking write",
    )

# ----------------------------------------------------------------------
# Field contracts
# Fields we explicitly map to silver columns — any that disappear from
# the API will halt the pipeline; any new ones we don't know about will
# trigger a warning.
# ----------------------------------------------------------------------
REQUIRED_GAME_FIELDS = {
    "id", "season", "gameType", "gameDate", "gameState",
    "awayTeam", "homeTeam", "plays",
}
KNOWN_GAME_FIELDS = REQUIRED_GAME_FIELDS | {
    "limitedScoring", "startTimeUTC", "easternUTCOffset", "venueUTCOffset",
    "gameScheduleState", "regPeriods", "venue", "venueLocation",
    "gameOutcome", "periodDescriptor", "tvBroadcasts", "shootoutInUse",
    "otInUse", "clock", "displayPeriod", "maxPeriods", "rosterSpots",
    "summary", "specialEvent",
}

REQUIRED_TEAM_FIELDS = {"id", "abbrev", "score", "commonName"}
KNOWN_TEAM_FIELDS = REQUIRED_TEAM_FIELDS | {
    "sog", "logo", "darkLogo", "placeName", "placeNameWithPreposition",
}

REQUIRED_PLAY_FIELDS = {"eventId", "typeCode", "typeDescKey", "sortOrder"}
KNOWN_PLAY_FIELDS = REQUIRED_PLAY_FIELDS | {
    "periodDescriptor", "timeInPeriod", "timeRemaining", "details",
    "situationCode", "homeTeamDefendingSide", "pptReplayUrl",
}

KNOWN_DETAIL_FIELDS = {
    "xCoord", "yCoord", "zoneCode", "eventOwnerTeamId", "shotType",
    "shootingPlayerId", "goalieInNetId",
    "scoringPlayerId", "scoringPlayerTotal",
    "assist1PlayerId", "assist1PlayerTotal",
    "assist2PlayerId", "assist2PlayerTotal",
    "assist3PlayerId", "assist3PlayerTotal",
    "awayScore", "homeScore", "awaySOG", "homeSOG",
    "hittingPlayerId", "hitteePlayerId",
    "blockingPlayerId",
    "winningPlayerId", "losingPlayerId",
    "playerId",
    "typeCode", "descKey", "duration",
    "committedByPlayerId", "drawnByPlayerId", "servedByPlayerId",
    "reason", "secondaryReason",
    "highlightClip", "highlightClipFr",
    "highlightClipSharingUrl", "highlightClipSharingUrlFr",
    "discreteClip", "discreteClipFr",
}

# Dedup set — each warning string fires at most once per run
_seen_warnings: set = set()

# ----------------------------------------------------------------------
# Ensure silver tables exist
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.silver")

# nhl_plays used to be one wide table with the game context and player names
# on every play. Its columns don't fit the normalised layout, so it has to be
# rebuilt from bronze rather than altered in place.
_old_layout = spark.sql(f"""
    SELECT 1 FROM nhl.information_schema.columns
    WHERE table_schema = 'silver' AND table_name = 'nhl_plays' AND column_name = 'season'
""").collect()
if _old_layout:
    msg = (
        f"{PLAYS_TABLE} still has the old denormalised layout. Rebuild silver from bronze: "
        f"DROP TABLE {PLAYS_TABLE}; DROP TABLE IF EXISTS {PROCESSED_TABLE}; then re-run this script."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def _ddl(schema: StructType) -> str:
    return ",\n        ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in schema.fields)


# Clustering on game_id keeps the per-game replaceWhere (batch and live) and
# the gold joins from scanning whole tables.
for table, schema in [
    (GAMES_TABLE,   GAMES_SCHEMA),
    (ROSTERS_TABLE, ROSTERS_SCHEMA),
    (PLAYS_TABLE,   PLAYS_SCHEMA),
]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
        {_ddl(schema)}
        ) USING DELTA
        CLUSTER BY (game_id)
    """)

PROCESSED_SCHEMA = StructType([
    StructField("bronze_event_id", LongType()),
    StructField("game_id",         LongType()),
    StructField("play_count",      IntegerType()),   # NULL = duplicate bronze row, superseded
    StructField("processed_at",    TimestampType()),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {PROCESSED_TABLE} (
        bronze_event_id BIGINT,
        game_id         BIGINT,
        play_count      INT,
        processed_at    TIMESTAMP
    ) USING DELTA
""")

logger.info("Silver tables ready.")

# ----------------------------------------------------------------------
# Schema validation
# ----------------------------------------------------------------------
def validate_game(game_id: int, data: dict) -> None:
    """
    Warns on new unmapped fields (data not yet captured in silver).
    Raises ValueError — halting the pipeline — if a required mapped
    field has been removed or renamed in the source API.
    """
    errors = []

    # --- Game level ---
    for f in sorted(REQUIRED_GAME_FIELDS - data.keys()):
        errors.append(f"required game field '{f}' is missing")

    for f in sorted(data.keys() - KNOWN_GAME_FIELDS):
        key = f"game:{f}"
        if key not in _seen_warnings:
            logger.warning(
                f"NEW UNMAPPED FIELD — game level: '{f}' "
                f"(game_id={game_id}). Add to KNOWN_GAME_FIELDS and silver schema to capture it."
            )
            _seen_warnings.add(key)

    # --- Team level ---
    for team_key in ("awayTeam", "homeTeam"):
        team = data.get(team_key) or {}
        for f in sorted(REQUIRED_TEAM_FIELDS - team.keys()):
            errors.append(f"required field '{f}' missing in {team_key}")
        for f in sorted(team.keys() - KNOWN_TEAM_FIELDS):
            key = f"{team_key}:{f}"
            if key not in _seen_warnings:
                logger.warning(
                    f"NEW UNMAPPED FIELD — {team_key}: '{f}' "
                    f"(game_id={game_id}). Add to KNOWN_TEAM_FIELDS and silver schema to capture it."
                )
                _seen_warnings.add(key)

    # --- Play level (deduplicated across all plays in this game) ---
    unknown_play_fields: set = set()
    unknown_detail_fields: set = set()

    for play in data.get("plays") or []:
        for f in sorted(REQUIRED_PLAY_FIELDS - play.keys()):
            errors.append(
                f"required play field '{f}' missing "
                f"(eventId={play.get('eventId', '?')})"
            )
        unknown_play_fields   |= play.keys() - KNOWN_PLAY_FIELDS
        unknown_detail_fields |= (play.get("details") or {}).keys() - KNOWN_DETAIL_FIELDS

    for f in sorted(unknown_play_fields):
        key = f"play:{f}"
        if key not in _seen_warnings:
            logger.warning(
                f"NEW UNMAPPED FIELD — play level: '{f}' "
                f"(game_id={game_id}). Add to KNOWN_PLAY_FIELDS and silver schema to capture it."
            )
            _seen_warnings.add(key)

    for f in sorted(unknown_detail_fields):
        key = f"detail:{f}"
        if key not in _seen_warnings:
            logger.warning(
                f"NEW UNMAPPED FIELD — play details: '{f}' "
                f"(game_id={game_id}). Add to KNOWN_DETAIL_FIELDS and silver schema to capture it."
            )
            _seen_warnings.add(key)

    # --- Stop pipeline on any missing required field ---
    if errors:
        msg = (
            f"PIPELINE STOPPED — game_id={game_id} is missing required mapped fields: "
            + ", ".join(errors)
            + ". A previously mapped API field may have been renamed or removed."
        )
        logger.error(msg)
        raise ValueError(msg)


# ----------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Guard against a stale tracker: if silver was dropped/emptied but the
    # tracker wasn't, every game would be skipped and silver would stay empty.
    if not spark.sql(f"SELECT 1 FROM {GAMES_TABLE} LIMIT 1").collect():
        logger.warning(f"{GAMES_TABLE} is empty — clearing {PROCESSED_TABLE} so every game is processed")
        spark.sql(f"TRUNCATE TABLE {PROCESSED_TABLE}")

    # Only bronze rows not handled by a previous run. Newest first, so if a game
    # was ever re-loaded into bronze, its latest row wins and older ones are skipped.
    pending_event_ids = sorted(
        (
            r.event_id
            for r in spark.sql(f"""
                SELECT b.event_id
                FROM nhl.bronze.nhl_events b
                LEFT ANTI JOIN {PROCESSED_TABLE} p ON b.event_id = p.bronze_event_id
            """).collect()
        ),
        reverse=True,
    )
    logger.info(f"Bronze rows not yet processed: {len(pending_event_ids)}")

    chunk_games   = []    # one row per game this chunk replaces in silver
    chunk_rosters = []
    chunk_plays   = []
    chunk_tracking = []   # (bronze_event_id, game_id, play_count, processed_at) for PROCESSED_TABLE
    seen_game_ids = set()
    processed = skipped = errors = 0

    for batch_start in range(0, len(pending_event_ids), BRONZE_BATCH_SIZE):
        batch_ids = pending_event_ids[batch_start:batch_start + BRONZE_BATCH_SIZE]
        ids_csv   = ",".join(str(eid) for eid in batch_ids)

        batch_rows = spark.sql(
            f"SELECT event_id, ingestion_timestamp, raw_json "
            f"FROM nhl.bronze.nhl_events WHERE event_id IN ({ids_csv})"
        ).collect()

        for row in sorted(batch_rows, key=lambda r: r.event_id, reverse=True):
            try:
                data    = json.loads(row.raw_json)
                game_id = data.get("id")
                if not game_id:
                    continue

                if game_id in seen_game_ids:
                    # Older duplicate bronze row for a game handled this run
                    chunk_tracking.append((row.event_id, game_id, None, RUN_TIMESTAMP))
                    skipped += 1
                    continue

                validate_game(game_id, data)  # warns on new fields; raises on missing required fields

                plays = parse_plays(data, "batch", RUN_TIMESTAMP)
                chunk_plays.extend(plays)
                chunk_rosters.extend(parse_roster(data, "batch", RUN_TIMESTAMP))
                chunk_games.append(parse_game(data, row.ingestion_timestamp, "batch", RUN_TIMESTAMP))
                chunk_tracking.append((row.event_id, game_id, len(plays), RUN_TIMESTAMP))
                seen_game_ids.add(game_id)
                processed += 1

                if processed % CHUNK_SIZE == 0:
                    logger.info(f"Flushing chunk — {processed} games, {len(chunk_plays)} play rows...")
                    _flush(chunk_games, chunk_rosters, chunk_plays, chunk_tracking)
                    chunk_games, chunk_rosters, chunk_plays, chunk_tracking = [], [], [], []

            except ValueError:
                raise  # missing required field — halt immediately
            except Exception as e:
                logger.error(f"Failed on bronze row (event_id={row.event_id}): {e}")
                errors += 1

    if chunk_tracking:
        logger.info(f"Flushing final chunk — {len(chunk_plays)} play rows...")
        _flush(chunk_games, chunk_rosters, chunk_plays, chunk_tracking)

    logger.info(f"Done. Processed={processed} | Skipped={skipped} | Errors={errors}")
