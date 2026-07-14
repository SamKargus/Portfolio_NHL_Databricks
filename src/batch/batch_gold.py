"""
Gold layer — a Kimball-style star schema over NHL play-by-play data.

Silver (nhl.silver.nhl_plays) is intentionally left un-modelled: it is a flat,
denormalised, name-only view of every play. Gold is where the dimensional model
lives. Because silver deliberately carries player *names* but no player *ids*,
this layer is built directly from the bronze JSON (nhl.bronze.nhl_events), which
is the only place that still holds real NHL player ids (in each play's `details`
and in `rosterSpots`). Linking facts to players by name would re-introduce the
duplicate-name collisions the id-based model exists to avoid, so gold re-parses
bronze rather than reading silver.

Grain & paradigm
----------------
A single atomic fact table at *play* grain, surrounded by conformed dimensions.
Natural business keys (real player_id / team_id / game_id / yyyymmdd date_key)
are used as the join keys — friendlier for ad-hoc SQL than opaque surrogates,
and collision-free.

        dim_date ─┐                 ┌─ dim_team
                  ├──  fact_event  ─┤
        dim_game ─┘                 ├─ dim_player  (role-playing)
   dim_event_type ─────────────────┘

Tables produced (all fully overwritten each run — idempotent, safe to re-run):
  nhl.gold.dim_player      one row per player_id
  nhl.gold.dim_team        one row per team_id
  nhl.gold.dim_game        one row per game_id
  nhl.gold.dim_date        one row per calendar date present in the data
  nhl.gold.dim_event_type  one row per event type_code
  nhl.gold.fact_event      one row per play; real player-id FK per role

The parsing is fully distributed (from_json + explode) — no driver-side JSON
work, so there is no OOM risk regardless of game count.
"""

import logging
from pathlib import Path
from datetime import datetime

from pyspark.sql import functions as F, types as T, Window

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"batch_gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("batch_gold")
logger.info(f"batch_gold started — log: {log_file}")

BRONZE_TABLE = "nhl.bronze.nhl_events"

DIM_PLAYER_TABLE     = "nhl.gold.dim_player"
DIM_TEAM_TABLE       = "nhl.gold.dim_team"
DIM_GAME_TABLE       = "nhl.gold.dim_game"
DIM_DATE_TABLE       = "nhl.gold.dim_date"
DIM_EVENT_TYPE_TABLE = "nhl.gold.dim_event_type"
FACT_EVENT_TABLE     = "nhl.gold.fact_event"

# Legacy tables from the previous, non-dimensional gold design — superseded by
# the star schema below and fully reproducible, so we drop them for a clean layer.
LEGACY_TABLES = [
    "nhl.gold.player_stats",
    "nhl.gold.team_stats",
    "nhl.gold.dim_players",
]

# ----------------------------------------------------------------------
# JSON schema for the bronze raw_json blob (only the fields we consume;
# from_json silently ignores everything else). Localised NHL strings are
# {"default": "..."} objects.
# ----------------------------------------------------------------------
_LOC = T.StructType([T.StructField("default", T.StringType())])

_TEAM = T.StructType([
    T.StructField("id",         T.IntegerType()),
    T.StructField("abbrev",     T.StringType()),
    T.StructField("commonName", _LOC),
    T.StructField("placeName",  _LOC),
    T.StructField("score",      T.IntegerType()),
    T.StructField("sog",        T.IntegerType()),
    T.StructField("logo",       T.StringType()),
    T.StructField("darkLogo",   T.StringType()),
])

_ROSTER_SPOT = T.StructType([
    T.StructField("teamId",        T.IntegerType()),
    T.StructField("playerId",      T.LongType()),
    T.StructField("firstName",     _LOC),
    T.StructField("lastName",      _LOC),
    T.StructField("sweaterNumber", T.IntegerType()),
    T.StructField("positionCode",  T.StringType()),
    T.StructField("headshot",      T.StringType()),
])

_PERIOD = T.StructType([
    T.StructField("number",     T.IntegerType()),
    T.StructField("periodType", T.StringType()),
])

_DETAILS = T.StructType([
    T.StructField("xCoord",              T.IntegerType()),
    T.StructField("yCoord",              T.IntegerType()),
    T.StructField("zoneCode",            T.StringType()),
    T.StructField("eventOwnerTeamId",    T.IntegerType()),
    T.StructField("shotType",            T.StringType()),
    # player-role ids
    T.StructField("scoringPlayerId",     T.LongType()),
    T.StructField("assist1PlayerId",     T.LongType()),
    T.StructField("assist2PlayerId",     T.LongType()),
    T.StructField("assist3PlayerId",     T.LongType()),
    T.StructField("shootingPlayerId",    T.LongType()),
    T.StructField("goalieInNetId",       T.LongType()),
    T.StructField("hittingPlayerId",     T.LongType()),
    T.StructField("hitteePlayerId",      T.LongType()),
    T.StructField("blockingPlayerId",    T.LongType()),
    T.StructField("winningPlayerId",     T.LongType()),
    T.StructField("losingPlayerId",      T.LongType()),
    T.StructField("committedByPlayerId", T.LongType()),
    T.StructField("drawnByPlayerId",     T.LongType()),
    T.StructField("servedByPlayerId",    T.LongType()),
    T.StructField("playerId",            T.LongType()),
    # running player/game totals
    T.StructField("scoringPlayerTotal",  T.IntegerType()),
    T.StructField("assist1PlayerTotal",  T.IntegerType()),
    T.StructField("assist2PlayerTotal",  T.IntegerType()),
    T.StructField("assist3PlayerTotal",  T.IntegerType()),
    T.StructField("awayScore",           T.IntegerType()),
    T.StructField("homeScore",           T.IntegerType()),
    T.StructField("awaySOG",             T.IntegerType()),
    T.StructField("homeSOG",             T.IntegerType()),
    # penalties
    T.StructField("typeCode",            T.StringType()),
    T.StructField("descKey",             T.StringType()),
    T.StructField("duration",            T.IntegerType()),
    # stoppages
    T.StructField("reason",              T.StringType()),
    T.StructField("secondaryReason",     T.StringType()),
])

_PLAY = T.StructType([
    T.StructField("eventId",               T.IntegerType()),
    T.StructField("periodDescriptor",      _PERIOD),
    T.StructField("timeInPeriod",          T.StringType()),
    T.StructField("timeRemaining",         T.StringType()),
    T.StructField("situationCode",         T.StringType()),
    T.StructField("homeTeamDefendingSide", T.StringType()),
    T.StructField("typeCode",              T.IntegerType()),
    T.StructField("typeDescKey",           T.StringType()),
    T.StructField("sortOrder",             T.IntegerType()),
    T.StructField("details",               _DETAILS),
])

_GAME = T.StructType([
    T.StructField("id",                T.LongType()),
    T.StructField("season",            T.IntegerType()),
    T.StructField("gameType",          T.IntegerType()),
    T.StructField("gameDate",          T.StringType()),
    T.StructField("startTimeUTC",      T.StringType()),
    T.StructField("gameState",         T.StringType()),
    T.StructField("gameScheduleState", T.StringType()),
    T.StructField("limitedScoring",    T.BooleanType()),
    T.StructField("regPeriods",        T.IntegerType()),
    T.StructField("venue",             _LOC),
    T.StructField("venueLocation",     _LOC),
    T.StructField("gameOutcome",       T.StructType([T.StructField("lastPeriodType", T.StringType())])),
    T.StructField("awayTeam",          _TEAM),
    T.StructField("homeTeam",          _TEAM),
    T.StructField("rosterSpots",       T.ArrayType(_ROSTER_SPOT)),
    T.StructField("plays",             T.ArrayType(_PLAY)),
])


# ----------------------------------------------------------------------
# Parse bronze once → one canonical row per game
# ----------------------------------------------------------------------
def load_canonical_games(spark):
    """
    Parse every bronze JSON blob and return exactly one row per game_id
    (the most recently ingested, in case a game was re-loaded), with a
    derived DATE game_date and integer date_key (yyyymmdd).
    """
    parsed = (
        spark.table(BRONZE_TABLE)
        .select("ingestion_timestamp", F.from_json("raw_json", _GAME).alias("g"))
        .select("ingestion_timestamp", "g.*")
        .filter(F.col("id").isNotNull())
        .withColumn("game_date", F.to_date("gameDate"))
        .withColumn("date_key", F.date_format("game_date", "yyyyMMdd").cast("int"))
    )

    latest = Window.partitionBy("id").orderBy(F.col("ingestion_timestamp").desc_nulls_last())
    return (
        parsed
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ----------------------------------------------------------------------
# Dimensions
# ----------------------------------------------------------------------
def build_dim_player(games):
    """One row per player_id, with the most recently seen descriptive attributes."""
    spots = (
        games
        .select("game_date", F.explode("rosterSpots").alias("s"))
        .select(
            F.col("game_date"),
            F.col("s.playerId").alias("player_id"),
            F.concat_ws(
                " ",
                F.col("s.firstName.default"),
                F.col("s.lastName.default"),
            ).alias("full_name"),
            F.col("s.firstName.default").alias("first_name"),
            F.col("s.lastName.default").alias("last_name"),
            F.col("s.positionCode").alias("position_code"),
            F.col("s.sweaterNumber").alias("sweater_number"),
            F.col("s.teamId").alias("team_id"),
            F.col("s.headshot").alias("headshot_url"),
        )
        .filter(F.col("player_id").isNotNull())
    )

    bounds = spots.groupBy("player_id").agg(
        F.min("game_date").alias("first_seen_date"),
        F.max("game_date").alias("last_seen_date"),
    )

    recent = Window.partitionBy("player_id").orderBy(F.col("game_date").desc_nulls_last())
    most_recent = (
        spots
        .withColumn("_rn", F.row_number().over(recent))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "game_date")
    )

    return (
        most_recent
        .join(bounds, "player_id", "left")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "player_id", "full_name", "first_name", "last_name",
            "position_code", "sweater_number", "team_id", "headshot_url",
            "first_seen_date", "last_seen_date", "ingestion_timestamp",
        )
    )


def build_dim_team(games):
    """One row per team_id, most recent descriptive attributes across home/away."""
    def _side(side):
        t = f"{side}Team"
        return games.select(
            F.col("game_date"),
            F.col(f"{t}.id").alias("team_id"),
            F.col(f"{t}.abbrev").alias("abbrev"),
            F.col(f"{t}.commonName.default").alias("team_name"),
            F.col(f"{t}.placeName.default").alias("place_name"),
            F.col(f"{t}.logo").alias("logo_url"),
            F.col(f"{t}.darkLogo").alias("dark_logo_url"),
        )

    teams = _side("home").union(_side("away")).filter(F.col("team_id").isNotNull())

    recent = Window.partitionBy("team_id").orderBy(F.col("game_date").desc_nulls_last())
    return (
        teams
        .withColumn("_rn", F.row_number().over(recent))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "game_date")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "team_id", "abbrev", "team_name", "place_name",
            "logo_url", "dark_logo_url", "ingestion_timestamp",
        )
    )


def build_dim_game(games):
    """One row per game_id holding all game-level context."""
    return (
        games.select(
            F.col("id").alias("game_id"),
            F.col("season"),
            F.col("gameType").alias("game_type"),
            F.col("game_date"),
            F.col("date_key"),
            F.to_timestamp("startTimeUTC").alias("start_time_utc"),
            F.col("venue.default").alias("venue"),
            F.col("venueLocation.default").alias("venue_location"),
            F.col("gameState").alias("game_state"),
            F.col("gameScheduleState").alias("game_schedule_state"),
            F.col("limitedScoring").alias("limited_scoring"),
            F.col("regPeriods").alias("reg_periods"),
            F.col("gameOutcome.lastPeriodType").alias("last_period_type"),
            F.col("homeTeam.id").alias("home_team_id"),
            F.col("awayTeam.id").alias("away_team_id"),
            F.col("homeTeam.score").alias("home_final_score"),
            F.col("awayTeam.score").alias("away_final_score"),
            F.col("homeTeam.sog").alias("home_final_sog"),
            F.col("awayTeam.sog").alias("away_final_sog"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


def build_dim_date(games):
    """Standard calendar dimension for every date present in the data."""
    return (
        games
        .select("game_date")
        .filter(F.col("game_date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format("game_date", "yyyyMMdd").cast("int"))
        .withColumn("year",         F.year("game_date"))
        .withColumn("month",        F.month("game_date"))
        .withColumn("day",          F.dayofmonth("game_date"))
        .withColumn("quarter",      F.quarter("game_date"))
        .withColumn("week_of_year", F.weekofyear("game_date"))
        .withColumn("day_of_week",  F.dayofweek("game_date"))  # 1=Sun … 7=Sat
        .withColumn("day_name",     F.date_format("game_date", "EEEE"))
        .withColumn("month_name",   F.date_format("game_date", "MMMM"))
        .withColumn("is_weekend",   F.dayofweek("game_date").isin(1, 7))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "date_key", F.col("game_date").alias("full_date"),
            "year", "quarter", "month", "month_name",
            "week_of_year", "day", "day_of_week", "day_name",
            "is_weekend", "ingestion_timestamp",
        )
    )


# ----------------------------------------------------------------------
# Fact table (play grain) + the event-type dimension derived from it
# ----------------------------------------------------------------------
def build_fact_event(games):
    """
    One row per play. Player-role columns carry the real NHL player_id and act
    as role-playing foreign keys into dim_player; game_id → dim_game,
    date_key → dim_date, type_code → dim_event_type, event_owner_team_id →
    dim_team. (game_id, event_id) is the natural grain.
    """
    exploded = games.select(
        F.col("id").alias("game_id"),
        F.col("date_key"),
        F.explode("plays").alias("p"),
    )

    d = F.col("p.details")
    return (
        exploded
        .select(
            # --- foreign keys ---
            F.col("game_id"),
            F.col("date_key"),
            F.col("p.typeCode").alias("type_code"),
            d.getField("eventOwnerTeamId").alias("event_owner_team_id"),
            # --- player-role foreign keys (→ dim_player) ---
            d.getField("scoringPlayerId").alias("scoring_player_id"),
            d.getField("assist1PlayerId").alias("assist1_player_id"),
            d.getField("assist2PlayerId").alias("assist2_player_id"),
            d.getField("assist3PlayerId").alias("assist3_player_id"),
            d.getField("shootingPlayerId").alias("shooting_player_id"),
            d.getField("goalieInNetId").alias("goalie_in_net_id"),
            d.getField("hittingPlayerId").alias("hitting_player_id"),
            d.getField("hitteePlayerId").alias("hittee_player_id"),
            d.getField("blockingPlayerId").alias("blocking_player_id"),
            d.getField("winningPlayerId").alias("winning_player_id"),
            d.getField("losingPlayerId").alias("losing_player_id"),
            d.getField("committedByPlayerId").alias("committed_by_player_id"),
            d.getField("drawnByPlayerId").alias("drawn_by_player_id"),
            d.getField("servedByPlayerId").alias("served_by_player_id"),
            d.getField("playerId").alias("player_id"),
            # --- degenerate dimensions / play descriptors ---
            F.col("p.eventId").alias("event_id"),
            F.col("p.sortOrder").alias("sort_order"),
            F.col("p.typeDescKey").alias("type_desc_key"),
            F.col("p.periodDescriptor.number").alias("period_number"),
            F.col("p.periodDescriptor.periodType").alias("period_type"),
            F.col("p.timeInPeriod").alias("time_in_period"),
            F.col("p.timeRemaining").alias("time_remaining"),
            F.col("p.situationCode").alias("situation_code"),
            F.col("p.homeTeamDefendingSide").alias("home_team_defending_side"),
            d.getField("xCoord").alias("x_coord"),
            d.getField("yCoord").alias("y_coord"),
            d.getField("zoneCode").alias("zone_code"),
            d.getField("shotType").alias("shot_type"),
            # --- measures ---
            d.getField("scoringPlayerTotal").alias("scoring_player_total"),
            d.getField("assist1PlayerTotal").alias("assist1_player_total"),
            d.getField("assist2PlayerTotal").alias("assist2_player_total"),
            d.getField("assist3PlayerTotal").alias("assist3_player_total"),
            d.getField("awayScore").alias("away_score"),
            d.getField("homeScore").alias("home_score"),
            d.getField("awaySOG").alias("away_sog"),
            d.getField("homeSOG").alias("home_sog"),
            d.getField("typeCode").alias("penalty_type_code"),
            d.getField("descKey").alias("penalty_desc_key"),
            d.getField("duration").alias("penalty_duration"),
            d.getField("reason").alias("stoppage_reason"),
            d.getField("secondaryReason").alias("secondary_stoppage_reason"),
        )
        .filter(F.col("event_id").isNotNull())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def build_dim_event_type(fact_event):
    """One row per event type_code, derived from the fact table."""
    recent = Window.partitionBy("type_code").orderBy(F.col("type_desc_key").asc_nulls_last())
    return (
        fact_event
        .select("type_code", "type_desc_key")
        .filter(F.col("type_code").isNotNull())
        .withColumn("_rn", F.row_number().over(recent))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select("type_code", "type_desc_key", "ingestion_timestamp")
    )


# ----------------------------------------------------------------------
# Write helper
# ----------------------------------------------------------------------
def overwrite_table(df, table):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    logger.info(f"Written: {table}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.gold")
logger.info("Gold schema ready.")

for legacy in LEGACY_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {legacy}")
    logger.info(f"Dropped legacy table (superseded by star schema): {legacy}")

games = load_canonical_games(spark).cache()
logger.info(f"Canonical games parsed: {games.count()}")

logger.info("Building dim_player…")
overwrite_table(build_dim_player(games), DIM_PLAYER_TABLE)

logger.info("Building dim_team…")
overwrite_table(build_dim_team(games), DIM_TEAM_TABLE)

logger.info("Building dim_game…")
overwrite_table(build_dim_game(games), DIM_GAME_TABLE)

logger.info("Building dim_date…")
overwrite_table(build_dim_date(games), DIM_DATE_TABLE)

logger.info("Building fact_event…")
fact_event = build_fact_event(games).cache()
overwrite_table(fact_event, FACT_EVENT_TABLE)
logger.info(f"fact_event rows: {fact_event.count()}")

logger.info("Building dim_event_type…")
overwrite_table(build_dim_event_type(fact_event), DIM_EVENT_TYPE_TABLE)

fact_event.unpersist()
games.unpersist()

logger.info("batch_gold complete.")
