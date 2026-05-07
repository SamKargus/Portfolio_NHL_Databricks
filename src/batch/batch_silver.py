import json
import logging
from pathlib import Path
from datetime import datetime, date as date_type
from pyspark.sql import Row
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
# Silver table schemas
# ----------------------------------------------------------------------
GAMES_SCHEMA = StructType([
    StructField("game_id",              LongType()),
    StructField("season",               IntegerType()),
    StructField("game_type",            IntegerType()),
    StructField("limited_scoring",      BooleanType()),
    StructField("game_date",            DateType()),
    StructField("venue",                StringType()),
    StructField("venue_location",       StringType()),
    StructField("start_time_utc",       TimestampType()),
    StructField("eastern_utc_offset",   StringType()),
    StructField("venue_utc_offset",     StringType()),
    StructField("game_state",           StringType()),
    StructField("game_schedule_state",  StringType()),
    StructField("away_team_id",         IntegerType()),
    StructField("away_team_abbrev",     StringType()),
    StructField("away_team_name",       StringType()),
    StructField("away_score",           IntegerType()),
    StructField("away_sog",             IntegerType()),
    StructField("home_team_id",         IntegerType()),
    StructField("home_team_abbrev",     StringType()),
    StructField("home_team_name",       StringType()),
    StructField("home_score",           IntegerType()),
    StructField("home_sog",             IntegerType()),
    StructField("period_number",        IntegerType()),
    StructField("period_type",          StringType()),
    StructField("last_period_type",     StringType()),
    StructField("reg_periods",          IntegerType()),
    StructField("ingestion_timestamp",  TimestampType()),
])

PLAYS_SCHEMA = StructType([
    StructField("game_id",                  LongType()),
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
    StructField("ingestion_timestamp",      TimestampType()),
])

ROSTER_SCHEMA = StructType([
    StructField("game_id",              LongType()),
    StructField("team_id",              IntegerType()),
    StructField("player_id",            IntegerType()),
    StructField("first_name",           StringType()),
    StructField("last_name",            StringType()),
    StructField("sweater_number",       IntegerType()),
    StructField("position_code",        StringType()),
    StructField("ingestion_timestamp",  TimestampType()),
])

# ----------------------------------------------------------------------
# Ensure silver tables exist
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nhl.silver.games (
        game_id             BIGINT,
        season              INT,
        game_type           INT,
        limited_scoring     BOOLEAN,
        game_date           DATE,
        venue               STRING,
        venue_location      STRING,
        start_time_utc      TIMESTAMP,
        eastern_utc_offset  STRING,
        venue_utc_offset    STRING,
        game_state          STRING,
        game_schedule_state STRING,
        away_team_id        INT,
        away_team_abbrev    STRING,
        away_team_name      STRING,
        away_score          INT,
        away_sog            INT,
        home_team_id        INT,
        home_team_abbrev    STRING,
        home_team_name      STRING,
        home_score          INT,
        home_sog            INT,
        period_number       INT,
        period_type         STRING,
        last_period_type    STRING,
        reg_periods         INT,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nhl.silver.plays (
        game_id                 BIGINT,
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
        ingestion_timestamp     TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nhl.silver.roster_spots (
        game_id             BIGINT,
        team_id             INT,
        player_id           INT,
        first_name          STRING,
        last_name           STRING,
        sweater_number      INT,
        position_code       STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

logger.info("Silver tables ready")

# ----------------------------------------------------------------------
# Parse helpers
# ----------------------------------------------------------------------
def parse_game(data: dict, ingestion_ts: datetime) -> dict:
    away    = data.get("awayTeam", {}) or {}
    home    = data.get("homeTeam", {}) or {}
    period  = data.get("periodDescriptor", {}) or {}
    outcome = data.get("gameOutcome", {}) or {}
    return {
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
        "away_score":           _int(away.get("score")),
        "away_sog":             _int(away.get("sog")),
        "home_team_id":         _int(home.get("id")),
        "home_team_abbrev":     _str(home.get("abbrev")),
        "home_team_name":       _str((home.get("commonName") or {}).get("default")),
        "home_score":           _int(home.get("score")),
        "home_sog":             _int(home.get("sog")),
        "period_number":        _int(period.get("number")),
        "period_type":          _str(period.get("periodType")),
        "last_period_type":     _str(outcome.get("lastPeriodType")),
        "reg_periods":          _int(data.get("regPeriods")),
        "ingestion_timestamp":  ingestion_ts,
    }


def parse_play(game_id: int, play: dict, ingestion_ts: datetime) -> dict:
    d      = play.get("details") or {}
    period = play.get("periodDescriptor") or {}
    return {
        "game_id":                  _long(game_id),
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
        "penalty_type_code":        _str(d.get("typeCode")),   # "MIN", "MAJ", etc.
        "penalty_desc_key":         _str(d.get("descKey")),
        "penalty_duration":         _int(d.get("duration")),
        "committed_by_player_id":   _int(d.get("committedByPlayerId")),
        "drawn_by_player_id":       _int(d.get("drawnByPlayerId")),
        "served_by_player_id":      _int(d.get("servedByPlayerId")),
        "stoppage_reason":          _str(d.get("reason")),
        "ingestion_timestamp":      ingestion_ts,
    }


def parse_roster_spot(game_id: int, spot: dict, ingestion_ts: datetime) -> dict:
    return {
        "game_id":              _long(game_id),
        "team_id":              _int(spot.get("teamId")),
        "player_id":            _int(spot.get("playerId")),
        "first_name":           _str((spot.get("firstName") or {}).get("default")),
        "last_name":            _str((spot.get("lastName") or {}).get("default")),
        "sweater_number":       _int(spot.get("sweaterNumber")),
        "position_code":        _str(spot.get("positionCode")),
        "ingestion_timestamp":  ingestion_ts,
    }

# ----------------------------------------------------------------------
# Flush accumulated rows to Delta
# ----------------------------------------------------------------------
def flush(game_rows: list, play_rows: list, roster_rows: list) -> None:
    if game_rows:
        spark.createDataFrame(game_rows, schema=GAMES_SCHEMA) \
            .write.format("delta").mode("append").saveAsTable("nhl.silver.games")
        logger.info(f"  → wrote {len(game_rows)} games")

    if play_rows:
        spark.createDataFrame(play_rows, schema=PLAYS_SCHEMA) \
            .write.format("delta").mode("append").saveAsTable("nhl.silver.plays")
        logger.info(f"  → wrote {len(play_rows)} plays")

    if roster_rows:
        spark.createDataFrame(roster_rows, schema=ROSTER_SCHEMA) \
            .write.format("delta").mode("append").saveAsTable("nhl.silver.roster_spots")
        logger.info(f"  → wrote {len(roster_rows)} roster spots")

# ----------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    existing_game_ids = {
        r.game_id
        for r in spark.sql("SELECT DISTINCT game_id FROM nhl.silver.games").collect()
    }
    logger.info(f"Games already in silver: {len(existing_game_ids)}")

    bronze_rows = spark.sql(
        "SELECT event_id, ingestion_timestamp, raw_json FROM nhl.bronze.nhl_events"
    ).collect()
    logger.info(f"Bronze rows to evaluate: {len(bronze_rows)}")

    game_buf, play_buf, roster_buf = [], [], []
    processed = skipped = errors = 0

    for row in bronze_rows:
        try:
            data     = json.loads(row.raw_json)
            game_id  = data.get("id")
            if not game_id:
                continue

            if game_id in existing_game_ids:
                skipped += 1
                continue

            ingestion_ts = row.ingestion_timestamp

            game_buf.append(parse_game(data, ingestion_ts))

            for play in data.get("plays") or []:
                play_buf.append(parse_play(game_id, play, ingestion_ts))

            for spot in data.get("rosterSpots") or []:
                roster_buf.append(parse_roster_spot(game_id, spot, ingestion_ts))

            existing_game_ids.add(game_id)
            processed += 1

            if processed % CHUNK_SIZE == 0:
                logger.info(f"Flushing chunk at {processed} games processed...")
                flush(game_buf, play_buf, roster_buf)
                game_buf, play_buf, roster_buf = [], [], []

        except Exception as e:
            logger.error(f"Failed on bronze row (event_id={row.event_id}): {e}")
            errors += 1

    # Final flush
    if game_buf or play_buf or roster_buf:
        logger.info("Flushing final chunk...")
        flush(game_buf, play_buf, roster_buf)

    logger.info(
        f"Done. Processed={processed} | Skipped={skipped} | Errors={errors}"
    )
