"""
Gold layer — a Kimball-style star schema over NHL play-by-play data.

Silver (nhl.silver.nhl_plays) is a flat, denormalised table of every play,
written by both the nightly batch and the live pipeline. Gold is where the
dimensional model lives.

  * fact_event is a VIEW over silver, so plays from games in progress show up
    in gold the moment the live pipeline merges them, alongside the history.
    Silver carries the real NHL player ids for every role, so the fact links to
    players by id — never by name, which would collide on duplicate names
    (e.g. two "Sebastian Aho").
  * The dimensions are rebuilt from the bronze JSON (nhl.bronze.nhl_events),
    which holds attributes silver doesn't (rosterSpots, logos, place names).

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

Objects produced (tables fully overwritten each run — idempotent, safe to re-run):
  nhl.gold.dim_player      one row per player_id (Type 1 — current attributes)
  nhl.gold.dim_player_team player↔team stint history (trades) with valid_from/
                           valid_to/is_current — the slowly-changing part
  nhl.gold.dim_team        one row per team_id
  nhl.gold.dim_game        one row per game_id
  nhl.gold.dim_date        one row per calendar day, 1917-01-01 to the end of
                           next year (so live games always have a date row)
  nhl.gold.dim_event_type  one row per event type_code
  nhl.gold.fact_event      VIEW over silver — one row per play, real player-id
                           FK per role, plus source/is_final lineage

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
SILVER_TABLE = "nhl.silver.nhl_plays"

DIM_PLAYER_TABLE     = "nhl.gold.dim_player"
DIM_PLAYER_TEAM_TABLE = "nhl.gold.dim_player_team"
DIM_TEAM_TABLE       = "nhl.gold.dim_team"
DIM_GAME_TABLE       = "nhl.gold.dim_game"
DIM_DATE_TABLE       = "nhl.gold.dim_date"
DIM_EVENT_TYPE_TABLE = "nhl.gold.dim_event_type"
FACT_EVENT_TABLE     = "nhl.gold.fact_event"

# Transient: holds the parsed-once canonical games so serverless compute (no
# cache/persist) still parses the bronze JSON only once. Dropped at the end.
STAGING_GAMES_TABLE  = "nhl.gold.staging_canonical_games"

# Legacy tables from the previous, non-dimensional gold design — superseded by
# the star schema below and fully reproducible, so we drop them for a clean layer.
LEGACY_TABLES = [
    "nhl.gold.player_stats",
    "nhl.gold.team_stats",
    "nhl.gold.dim_players",
]

# ----------------------------------------------------------------------
# JSON schema for the bronze raw_json blob — only the game-level fields the
# dimensions consume (from_json silently ignores everything else). Localised
# NHL strings are {"default": "..."} objects.
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
    # plays are not parsed here — fact_event reads them from silver
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


def build_dim_player_team(games):
    """
    Player↔team stint history (the slowly-changing part of a player's identity).
    One row per contiguous spell a player spent on a team, derived from per-game
    rosterSpots. Captures trades — including mid-season moves and a player
    returning to a former team (each spell is its own stint).

    Grain: (player_id, team_id, valid_from). Columns:
      player_id, team_id, valid_from, valid_to, games_in_stint, is_current
    """
    spots = (
        games
        .select("game_date", F.explode("rosterSpots").alias("s"))
        .select(
            F.col("game_date"),
            F.col("s.playerId").alias("player_id"),
            F.col("s.teamId").alias("team_id"),
        )
        .filter(
            F.col("player_id").isNotNull()
            & F.col("team_id").isNotNull()
            & F.col("game_date").isNotNull()
        )
    )

    # Collapse to one team per player per game (guards against any duplicate spots).
    one_per_game = Window.partitionBy("player_id", "game_date").orderBy(F.col("team_id"))
    per_game = (
        spots
        .withColumn("_rn", F.row_number().over(one_per_game))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Gaps-and-islands: a new stint begins whenever the team differs from the
    # team in the player's previous game (chronologically).
    order = Window.partitionBy("player_id").orderBy("game_date")
    flagged = (
        per_game
        .withColumn("prev_team", F.lag("team_id").over(order))
        .withColumn(
            "is_new_stint",
            (F.col("prev_team").isNull() | (F.col("prev_team") != F.col("team_id"))).cast("int"),
        )
        .withColumn("stint_id", F.sum("is_new_stint").over(order))  # running island id
    )

    stints = (
        flagged
        .groupBy("player_id", "stint_id", "team_id")
        .agg(
            F.min("game_date").alias("valid_from"),
            F.max("game_date").alias("valid_to"),
            F.count("*").alias("games_in_stint"),
        )
    )

    # The player's most recent stint is the current one.
    current = Window.partitionBy("player_id").orderBy(F.col("valid_from").desc())
    return (
        stints
        .withColumn("_rn", F.row_number().over(current))
        .withColumn("is_current", F.col("_rn") == 1)
        .drop("_rn", "stint_id")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "player_id", "team_id", "valid_from", "valid_to",
            "games_in_stint", "is_current", "ingestion_timestamp",
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


def build_dim_date(spark):
    """
    Standard calendar dimension: every day from the first NHL season to the end
    of next year. Generated rather than derived from games, so tonight's live
    games (not in bronze until they're final) still join to a date row.
    """
    return (
        spark.range(1)
        .select(F.expr(
            "explode(sequence(DATE'1917-01-01', make_date(year(current_date()) + 1, 12, 31), INTERVAL 1 DAY))"
        ).alias("game_date"))
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
# Fact view (play grain, over silver) + the event-type dimension derived from it
# ----------------------------------------------------------------------
def create_fact_event_view(spark):
    """
    One row per play, as a view over silver — batch history and live games in
    one place. Player-role columns carry the real NHL player_id and act as
    role-playing foreign keys into dim_player; game_id → dim_game,
    date_key → dim_date, type_code → dim_event_type, event_owner_team_id →
    dim_team. (game_id, event_id) is the natural grain.

    Lineage: source = 'batch' (final, from bronze) or 'live' (game in progress);
    is_final says whether the game was over when the row was written.
    """
    # fact_event used to be a table, and a view can't replace a table in place.
    existing = spark.sql("""
        SELECT table_type FROM nhl.information_schema.tables
        WHERE table_schema = 'gold' AND table_name = 'fact_event'
    """).collect()
    if existing and existing[0].table_type != "VIEW":
        spark.sql(f"DROP TABLE {FACT_EVENT_TABLE}")
        logger.info(f"Dropped old {FACT_EVENT_TABLE} table (replaced by a view over silver)")

    spark.sql(f"""
        CREATE OR REPLACE VIEW {FACT_EVENT_TABLE}
        COMMENT 'One row per play (game_id, event_id) — batch history + live games, over {SILVER_TABLE}'
        AS SELECT
            -- foreign keys
            game_id,
            CAST(date_format(game_date, 'yyyyMMdd') AS INT) AS date_key,
            type_code,
            event_owner_team_id,
            -- player-role foreign keys (→ dim_player)
            scoring_player_id,
            assist1_player_id,
            assist2_player_id,
            assist3_player_id,
            shooting_player_id,
            goalie_in_net_id,
            hitting_player_id,
            hittee_player_id,
            blocking_player_id,
            winning_player_id,
            losing_player_id,
            committed_by_player_id,
            drawn_by_player_id,
            served_by_player_id,
            player_id,
            -- degenerate dimensions / play descriptors
            event_id,
            sort_order,
            type_desc_key,
            period_number,
            period_type,
            time_in_period,
            time_remaining,
            situation_code,
            home_team_defending_side,
            x_coord,
            y_coord,
            zone_code,
            shot_type,
            -- measures
            scoring_player_total,
            assist1_player_total,
            assist2_player_total,
            assist3_player_total,
            away_score,
            home_score,
            away_sog,
            home_sog,
            penalty_type_code,
            penalty_desc_key,
            penalty_duration,
            stoppage_reason,
            secondary_stoppage_reason,
            -- lineage
            source,
            is_final,
            fetched_at,
            ingestion_timestamp
        FROM {SILVER_TABLE}
        WHERE event_id IS NOT NULL
    """)


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

# Serverless compute does not support cache()/persist(), so we materialise the
# parsed-once canonical games to a staging Delta table and read it back for every
# downstream build — this parses the bronze JSON a single time.
overwrite_table(load_canonical_games(spark), STAGING_GAMES_TABLE)
games = spark.table(STAGING_GAMES_TABLE)
logger.info(f"Canonical games parsed: {games.count()}")

logger.info("Building dim_player…")
overwrite_table(build_dim_player(games), DIM_PLAYER_TABLE)

logger.info("Building dim_player_team…")
overwrite_table(build_dim_player_team(games), DIM_PLAYER_TEAM_TABLE)

logger.info("Building dim_team…")
overwrite_table(build_dim_team(games), DIM_TEAM_TABLE)

logger.info("Building dim_game…")
overwrite_table(build_dim_game(games), DIM_GAME_TABLE)

logger.info("Building dim_date…")
overwrite_table(build_dim_date(spark), DIM_DATE_TABLE)

logger.info("Creating fact_event view over silver…")
create_fact_event_view(spark)
fact_event = spark.table(FACT_EVENT_TABLE)

logger.info("Building dim_event_type…")
overwrite_table(build_dim_event_type(fact_event), DIM_EVENT_TYPE_TABLE)

spark.sql(f"DROP TABLE IF EXISTS {STAGING_GAMES_TABLE}")
logger.info("Dropped staging table.")

logger.info("batch_gold complete.")
