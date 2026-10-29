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
from silver_parse import SILVER_TABLE, SILVER_SCHEMA, parse_plays, player_names  # noqa: E402

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

CHUNK_SIZE = 500         # flush play rows to Delta every N games
BRONZE_BATCH_SIZE = 50  # raw JSON rows fetched from bronze per batch
MAX_FLUSH_RETRIES = 5
FLUSH_RETRY_BASE_DELAY = 30  # seconds; doubles each attempt (30, 60, 120, 240, ...)

RUN_TIMESTAMP = datetime.now()  # when this run wrote to silver (ingestion_timestamp)


# One row per bronze row this script has handled — including games with no
# plays (most pre-2010 games), which never get a row in SILVER_TABLE and so
# would otherwise be re-read and re-parsed on every run.
# Drop it together with SILVER_TABLE when rebuilding silver.
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


def _flush(buf: list, game_ids: list, tracking: list) -> None:
    # replaceWhere swaps out every existing row for these games (e.g. rows the
    # live pipeline wrote while the game was on) in one atomic commit, so a
    # retry can never duplicate plays.
    if game_ids:  # empty when the chunk only held duplicate bronze rows
        predicate = f"game_id IN ({','.join(str(g) for g in game_ids)})"
        _with_retries(
            lambda: spark.createDataFrame(buf, schema=SILVER_SCHEMA)
                .write.format("delta").mode("overwrite")
                .option("replaceWhere", predicate)
                .saveAsTable(SILVER_TABLE),
            "Flush",
        )
    # Recorded only after the plays are written: if the run dies in between,
    # the chunk is simply re-processed next run (replaceWhere is idempotent).
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
# Ensure silver table exists
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nhl.silver.nhl_plays (
        game_id                 BIGINT,
        season                  INT,
        game_type               INT,
        limited_scoring         BOOLEAN,
        game_date               DATE,
        venue                   STRING,
        venue_location          STRING,
        start_time_utc          TIMESTAMP,
        eastern_utc_offset      STRING,
        venue_utc_offset        STRING,
        game_state              STRING,
        game_schedule_state     STRING,
        away_team_id            INT,
        away_team_abbrev        STRING,
        away_team_name          STRING,
        away_final_score        INT,
        away_final_sog          INT,
        home_team_id            INT,
        home_team_abbrev        STRING,
        home_team_name          STRING,
        home_final_score        INT,
        home_final_sog          INT,
        last_period_type        STRING,
        reg_periods             INT,
        special_event           STRING,
        event_id                INT,
        period_number           INT,
        period_type             STRING,
        time_in_period          STRING,
        time_remaining          STRING,
        type_code               INT,
        type_desc_key           STRING,
        sort_order              INT,
        x_coord                 INT,
        y_coord                 INT,
        zone_code               STRING,
        home_team_defending_side STRING,
        event_owner_team_id     INT,
        shot_type               STRING,
        scoring_player_total    INT,
        assist1_player_total    INT,
        assist2_player_total    INT,
        assist3_player_total    INT,
        away_score              INT,
        home_score              INT,
        away_sog                INT,
        home_sog                INT,
        penalty_type_code       STRING,
        penalty_desc_key        STRING,
        penalty_duration        INT,
        stoppage_reason         STRING,
        secondary_stoppage_reason STRING,
        situation_code           STRING,
        shooting_player_name     STRING,
        goalie_in_net_name       STRING,
        scoring_player_name      STRING,
        assist1_player_name      STRING,
        assist2_player_name      STRING,
        assist3_player_name      STRING,
        hitting_player_name      STRING,
        hittee_player_name       STRING,
        blocking_player_name     STRING,
        winning_player_name      STRING,
        losing_player_name       STRING,
        committed_by_player_name STRING,
        drawn_by_player_name     STRING,
        served_by_player_name    STRING,
        player_name              STRING,
        shooting_player_id       BIGINT,
        goalie_in_net_id         BIGINT,
        scoring_player_id        BIGINT,
        assist1_player_id        BIGINT,
        assist2_player_id        BIGINT,
        assist3_player_id        BIGINT,
        hitting_player_id        BIGINT,
        hittee_player_id         BIGINT,
        blocking_player_id       BIGINT,
        winning_player_id        BIGINT,
        losing_player_id         BIGINT,
        committed_by_player_id   BIGINT,
        drawn_by_player_id       BIGINT,
        served_by_player_id      BIGINT,
        player_id                BIGINT,
        highlight_clip               BIGINT,
        highlight_clip_fr            BIGINT,
        highlight_clip_sharing_url    STRING,
        highlight_clip_sharing_url_fr STRING,
        discrete_clip                BIGINT,
        discrete_clip_fr             BIGINT,
        ppt_replay_url               STRING,
        source                   STRING,
        is_final                 BOOLEAN,
        fetched_at               TIMESTAMP,
        ingestion_timestamp      TIMESTAMP
    ) USING DELTA
    CLUSTER BY (game_id)
""")

# Add columns introduced after the table was first created (safe to re-run)
for col_ddl in [
    "special_event            STRING",
    "situation_code           STRING",
    "assist3_player_total     INT",
    "shooting_player_name     STRING",
    "goalie_in_net_name       STRING",
    "scoring_player_name      STRING",
    "assist1_player_name      STRING",
    "assist2_player_name      STRING",
    "assist3_player_name      STRING",
    "hitting_player_name      STRING",
    "hittee_player_name       STRING",
    "blocking_player_name     STRING",
    "winning_player_name      STRING",
    "losing_player_name       STRING",
    "committed_by_player_name STRING",
    "drawn_by_player_name     STRING",
    "served_by_player_name    STRING",
    "player_name              STRING",
    "zone_code                STRING",
    "home_team_defending_side STRING",
    "secondary_stoppage_reason STRING",
    "highlight_clip               BIGINT",
    "highlight_clip_fr            BIGINT",
    "highlight_clip_sharing_url    STRING",
    "highlight_clip_sharing_url_fr STRING",
    "discrete_clip                BIGINT",
    "discrete_clip_fr             BIGINT",
    "ppt_replay_url               STRING",
    "shooting_player_id       BIGINT",
    "goalie_in_net_id         BIGINT",
    "scoring_player_id        BIGINT",
    "assist1_player_id        BIGINT",
    "assist2_player_id        BIGINT",
    "assist3_player_id        BIGINT",
    "hitting_player_id        BIGINT",
    "hittee_player_id         BIGINT",
    "blocking_player_id       BIGINT",
    "winning_player_id        BIGINT",
    "losing_player_id         BIGINT",
    "committed_by_player_id   BIGINT",
    "drawn_by_player_id       BIGINT",
    "served_by_player_id      BIGINT",
    "player_id                BIGINT",
    "source                   STRING",
    "is_final                 BOOLEAN",
    "fetched_at               TIMESTAMP",
]:
    try:
        spark.sql(f"ALTER TABLE nhl.silver.nhl_plays ADD COLUMN {col_ddl}")
    except Exception:
        pass  # column already exists

# Clustering on game_id keeps per-game replaceWhere (batch) and MERGE (live)
# from scanning the whole table. No-op if the table was created with it.
try:
    spark.sql("ALTER TABLE nhl.silver.nhl_plays CLUSTER BY (game_id)")
except Exception as e:
    logger.warning(f"Could not enable clustering on game_id: {e}")

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

logger.info("Silver table ready.")

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
    if not spark.sql(f"SELECT 1 FROM {SILVER_TABLE} LIMIT 1").collect():
        logger.warning(f"{SILVER_TABLE} is empty — clearing {PROCESSED_TABLE} so every game is processed")
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

    buf = []
    chunk_game_ids = []   # games whose rows this chunk replaces in silver
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

                plays = parse_plays(data, row.ingestion_timestamp, player_names(data), "batch", RUN_TIMESTAMP)
                buf.extend(plays)
                chunk_game_ids.append(game_id)
                chunk_tracking.append((row.event_id, game_id, len(plays), RUN_TIMESTAMP))
                seen_game_ids.add(game_id)
                processed += 1

                if processed % CHUNK_SIZE == 0:
                    logger.info(f"Flushing chunk — {processed} games, {len(buf)} play rows...")
                    _flush(buf, chunk_game_ids, chunk_tracking)
                    buf = []
                    chunk_game_ids = []
                    chunk_tracking = []

            except ValueError:
                raise  # missing required field — halt immediately
            except Exception as e:
                logger.error(f"Failed on bronze row (event_id={row.event_id}): {e}")
                errors += 1

    if chunk_tracking:
        logger.info(f"Flushing final chunk — {len(buf)} play rows...")
        _flush(buf, chunk_game_ids, chunk_tracking)

    logger.info(f"Done. Processed={processed} | Skipped={skipped} | Errors={errors}")
