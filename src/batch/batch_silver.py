import json
import logging
import time
from pathlib import Path
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType,
    DateType, TimestampType,
)

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
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

CHUNK_SIZE = 500  # flush to Delta every N games
MAX_FLUSH_RETRIES = 5
FLUSH_RETRY_BASE_DELAY = 30  # seconds; doubles each attempt (30, 60, 120, 240, ...)


def _flush(buf: list, table: str, schema) -> None:
    for attempt in range(1, MAX_FLUSH_RETRIES + 1):
        try:
            spark.createDataFrame(buf, schema=schema) \
                .write.format("delta").mode("append").saveAsTable(table)
            return
        except Exception as e:
            if attempt == MAX_FLUSH_RETRIES:
                raise
            delay = FLUSH_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"Flush attempt {attempt}/{MAX_FLUSH_RETRIES} failed "
                f"({type(e).__name__}), retrying in {delay}s..."
            )
            time.sleep(delay)

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
    "situationCode",
}

KNOWN_DETAIL_FIELDS = {
    "xCoord", "yCoord", "eventOwnerTeamId", "shotType",
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
    "reason",
}

# Dedup set — each warning string fires at most once per run
_seen_warnings: set = set()

# ----------------------------------------------------------------------
# Type helpers
# ----------------------------------------------------------------------
def _int(v):
    return int(v) if v is not None else None

def _long(v):
    return int(v) if v is not None else None

def _bool(v):
    return bool(v) if v is not None else None

def _str(v):
    return str(v) if v is not None else None

def _date(v):
    if v is None:
        return None
    if isinstance(v, str):
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    return v

def _ts(v):
    if v is None:
        return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
    return v

# ----------------------------------------------------------------------
# Schema — one row per play, game context denormalised onto every row
# ----------------------------------------------------------------------
SILVER_SCHEMA = StructType([
    # Game context
    StructField("game_id",                  LongType()),
    StructField("season",                   IntegerType()),
    StructField("game_type",                IntegerType()),
    StructField("limited_scoring",          BooleanType()),
    StructField("game_date",                DateType()),
    StructField("venue",                    StringType()),
    StructField("venue_location",           StringType()),
    StructField("start_time_utc",           TimestampType()),
    StructField("eastern_utc_offset",       StringType()),
    StructField("venue_utc_offset",         StringType()),
    StructField("game_state",               StringType()),
    StructField("game_schedule_state",      StringType()),
    StructField("away_team_id",             IntegerType()),
    StructField("away_team_abbrev",         StringType()),
    StructField("away_team_name",           StringType()),
    StructField("away_final_score",         IntegerType()),
    StructField("away_final_sog",           IntegerType()),
    StructField("home_team_id",             IntegerType()),
    StructField("home_team_abbrev",         StringType()),
    StructField("home_team_name",           StringType()),
    StructField("home_final_score",         IntegerType()),
    StructField("home_final_sog",           IntegerType()),
    StructField("last_period_type",         StringType()),
    StructField("reg_periods",              IntegerType()),
    StructField("special_event",            StringType()),
    # Play fields
    StructField("event_id",                 IntegerType()),
    StructField("period_number",            IntegerType()),
    StructField("period_type",              StringType()),
    StructField("time_in_period",           StringType()),
    StructField("time_remaining",           StringType()),
    StructField("type_code",                IntegerType()),
    StructField("type_desc_key",            StringType()),
    StructField("sort_order",               IntegerType()),
    StructField("x_coord",                  IntegerType()),
    StructField("y_coord",                  IntegerType()),
    StructField("event_owner_team_id",      IntegerType()),
    StructField("shot_type",                StringType()),
    StructField("shooting_player_id",       IntegerType()),
    StructField("goalie_in_net_id",         IntegerType()),
    StructField("scoring_player_id",        IntegerType()),
    StructField("scoring_player_total",     IntegerType()),
    StructField("assist1_player_id",        IntegerType()),
    StructField("assist1_player_total",     IntegerType()),
    StructField("assist2_player_id",        IntegerType()),
    StructField("assist2_player_total",     IntegerType()),
    StructField("assist3_player_id",        IntegerType()),
    StructField("assist3_player_total",     IntegerType()),
    StructField("away_score",               IntegerType()),
    StructField("home_score",               IntegerType()),
    StructField("away_sog",                 IntegerType()),
    StructField("home_sog",                 IntegerType()),
    StructField("hitting_player_id",        IntegerType()),
    StructField("hittee_player_id",         IntegerType()),
    StructField("blocking_player_id",       IntegerType()),
    StructField("winning_player_id",        IntegerType()),
    StructField("losing_player_id",         IntegerType()),
    StructField("player_id",                IntegerType()),
    StructField("penalty_type_code",        StringType()),
    StructField("penalty_desc_key",         StringType()),
    StructField("penalty_duration",         IntegerType()),
    StructField("committed_by_player_id",   IntegerType()),
    StructField("drawn_by_player_id",       IntegerType()),
    StructField("served_by_player_id",      IntegerType()),
    StructField("stoppage_reason",          StringType()),
    StructField("situation_code",           StringType()),
    StructField("ingestion_timestamp",      TimestampType()),
])

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
        event_owner_team_id     INT,
        shot_type               STRING,
        shooting_player_id      INT,
        goalie_in_net_id        INT,
        scoring_player_id       INT,
        scoring_player_total    INT,
        assist1_player_id       INT,
        assist1_player_total    INT,
        assist2_player_id       INT,
        assist2_player_total    INT,
        assist3_player_id       INT,
        assist3_player_total    INT,
        away_score              INT,
        home_score              INT,
        away_sog                INT,
        home_sog                INT,
        hitting_player_id       INT,
        hittee_player_id        INT,
        blocking_player_id      INT,
        winning_player_id       INT,
        losing_player_id        INT,
        player_id               INT,
        penalty_type_code       STRING,
        penalty_desc_key        STRING,
        penalty_duration        INT,
        committed_by_player_id  INT,
        drawn_by_player_id      INT,
        served_by_player_id     INT,
        stoppage_reason         STRING,
        situation_code          STRING,
        ingestion_timestamp     TIMESTAMP
    ) USING DELTA
""")

# Add columns introduced after the table was first created (safe to re-run)
for col_ddl in [
    "special_event        STRING",
    "situation_code       STRING",
    "assist3_player_id    INT",
    "assist3_player_total INT",
]:
    try:
        spark.sql(f"ALTER TABLE nhl.silver.nhl_plays ADD COLUMN {col_ddl}")
    except Exception:
        pass  # column already exists

logger.info("Table nhl.silver.nhl_plays is ready")

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
# Parse helper — one dict per play with game context embedded
# ----------------------------------------------------------------------
def parse_plays(data: dict, ingestion_ts: datetime) -> list:
    away    = data.get("awayTeam", {}) or {}
    home    = data.get("homeTeam", {}) or {}
    outcome = data.get("gameOutcome", {}) or {}

    game_ctx = {
        "game_id":              _long(data.get("id")),
        "season":               _int(data.get("season")),
        "game_type":            _int(data.get("gameType")),
        "limited_scoring":      _bool(data.get("limitedScoring")),
        "game_date":            _date(data.get("gameDate")),
        "venue":                _str((data.get("venue") or {}).get("default")),
        "venue_location":       _str((data.get("venueLocation") or {}).get("default")),
        "start_time_utc":       _ts(data.get("startTimeUTC")),
        "eastern_utc_offset":   _str(data.get("easternUTCOffset")),
        "venue_utc_offset":     _str(data.get("venueUTCOffset")),
        "game_state":           _str(data.get("gameState")),
        "game_schedule_state":  _str(data.get("gameScheduleState")),
        "away_team_id":         _int(away.get("id")),
        "away_team_abbrev":     _str(away.get("abbrev")),
        "away_team_name":       _str((away.get("commonName") or {}).get("default")),
        "away_final_score":     _int(away.get("score")),
        "away_final_sog":       _int(away.get("sog")),
        "home_team_id":         _int(home.get("id")),
        "home_team_abbrev":     _str(home.get("abbrev")),
        "home_team_name":       _str((home.get("commonName") or {}).get("default")),
        "home_final_score":     _int(home.get("score")),
        "home_final_sog":       _int(home.get("sog")),
        "last_period_type":     _str(outcome.get("lastPeriodType")),
        "reg_periods":          _int(data.get("regPeriods")),
        "special_event":        _str((data.get("specialEvent") or {}).get("default")
                                     if isinstance(data.get("specialEvent"), dict)
                                     else data.get("specialEvent")),
        "ingestion_timestamp":  ingestion_ts,
    }

    rows = []
    for play in data.get("plays") or []:
        d      = play.get("details") or {}
        period = play.get("periodDescriptor") or {}
        row = {
            **game_ctx,
            "event_id":                 _int(play.get("eventId")),
            "period_number":            _int(period.get("number")),
            "period_type":              _str(period.get("periodType")),
            "time_in_period":           _str(play.get("timeInPeriod")),
            "time_remaining":           _str(play.get("timeRemaining")),
            "type_code":                _int(play.get("typeCode")),
            "type_desc_key":            _str(play.get("typeDescKey")),
            "sort_order":               _int(play.get("sortOrder")),
            "x_coord":                  _int(d.get("xCoord")),
            "y_coord":                  _int(d.get("yCoord")),
            "event_owner_team_id":      _int(d.get("eventOwnerTeamId")),
            "shot_type":                _str(d.get("shotType")),
            "shooting_player_id":       _int(d.get("shootingPlayerId")),
            "goalie_in_net_id":         _int(d.get("goalieInNetId")),
            "scoring_player_id":        _int(d.get("scoringPlayerId")),
            "scoring_player_total":     _int(d.get("scoringPlayerTotal")),
            "assist1_player_id":        _int(d.get("assist1PlayerId")),
            "assist1_player_total":     _int(d.get("assist1PlayerTotal")),
            "assist2_player_id":        _int(d.get("assist2PlayerId")),
            "assist2_player_total":     _int(d.get("assist2PlayerTotal")),
            "assist3_player_id":        _int(d.get("assist3PlayerId")),
            "assist3_player_total":     _int(d.get("assist3PlayerTotal")),
            "away_score":               _int(d.get("awayScore")),
            "home_score":               _int(d.get("homeScore")),
            "away_sog":                 _int(d.get("awaySOG")),
            "home_sog":                 _int(d.get("homeSOG")),
            "hitting_player_id":        _int(d.get("hittingPlayerId")),
            "hittee_player_id":         _int(d.get("hitteePlayerId")),
            "blocking_player_id":       _int(d.get("blockingPlayerId")),
            "winning_player_id":        _int(d.get("winningPlayerId")),
            "losing_player_id":         _int(d.get("losingPlayerId")),
            "player_id":                _int(d.get("playerId")),
            "penalty_type_code":        _str(d.get("typeCode")),
            "penalty_desc_key":         _str(d.get("descKey")),
            "penalty_duration":         _int(d.get("duration")),
            "committed_by_player_id":   _int(d.get("committedByPlayerId")),
            "drawn_by_player_id":       _int(d.get("drawnByPlayerId")),
            "served_by_player_id":      _int(d.get("servedByPlayerId")),
            "stoppage_reason":          _str(d.get("reason")),
            "situation_code":           _str(play.get("situationCode")),
        }
        rows.append(row)
    return rows

# ----------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    existing_game_ids = {
        r.game_id
        for r in spark.sql("SELECT DISTINCT game_id FROM nhl.silver.nhl_plays").collect()
    }
    logger.info(f"Games already in silver: {len(existing_game_ids)}")

    bronze_rows = spark.sql(
        "SELECT event_id, ingestion_timestamp, raw_json FROM nhl.bronze.nhl_events"
    ).collect()
    logger.info(f"Bronze rows to evaluate: {len(bronze_rows)}")

    buf = []
    processed = skipped = errors = 0

    for row in bronze_rows:
        try:
            data    = json.loads(row.raw_json)
            game_id = data.get("id")
            if not game_id:
                continue

            if game_id in existing_game_ids:
                skipped += 1
                continue

            validate_game(game_id, data)  # warns on new fields; raises on missing required fields

            buf.extend(parse_plays(data, row.ingestion_timestamp))
            existing_game_ids.add(game_id)
            processed += 1

            if processed % CHUNK_SIZE == 0:
                logger.info(f"Flushing chunk — {processed} games, {len(buf)} play rows...")
                _flush(buf, "nhl.silver.nhl_plays", SILVER_SCHEMA)
                buf = []

        except ValueError:
            raise  # missing required field — halt immediately
        except Exception as e:
            logger.error(f"Failed on bronze row (event_id={row.event_id}): {e}")
            errors += 1

    if buf:
        logger.info(f"Flushing final chunk — {len(buf)} play rows...")
        _flush(buf, "nhl.silver.nhl_plays", SILVER_SCHEMA)

    logger.info(f"Done. Processed={processed} | Skipped={skipped} | Errors={errors}")
