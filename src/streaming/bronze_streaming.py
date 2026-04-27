"""
bronze_streaming.py
Auto Loader: reads NDJSON landing files → writes to Delta bronze tables.
Runs as a continuous Databricks Job task.

Landing directories (written by nhl_api_producer.py):
  dbfs:/mnt/nhl/landing/game_events/   → bronze/game_events  Delta table
  dbfs:/mnt/nhl/landing/player_stats/  → bronze/player_stats Delta table
  dbfs:/mnt/nhl/landing/game_state/    → game_scores         Delta table
  dbfs:/mnt/nhl/landing/players/       → players             Delta table
"""

import logging
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

_f = globals().get("__file__") or sys._getframe(0).f_code.co_filename; sys.path.insert(0, str(__import__("pathlib").Path(_f).parents[2]))
from src.utils.delta import (
    DB_CATALOG, DB_SCHEMA,
    ensure_schema, register_table,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("bronze_streaming")

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE = os.getenv("DELTA_BASE") or f"/Volumes/{spark.catalog.currentCatalog()}/nhl/nhl_data"
LANDING_EVENTS   = f"{_BASE}/landing/game_events"
LANDING_STATS    = f"{_BASE}/landing/player_stats"
LANDING_STATE    = f"{_BASE}/landing/game_state"
LANDING_PLAYERS  = f"{_BASE}/landing/players"

BRONZE_EVENTS    = f"{_BASE}/bronze/game_events"
BRONZE_STATS     = f"{_BASE}/bronze/player_stats"
GAME_SCORES      = f"{_BASE}/bronze/game_scores"
PLAYERS          = f"{_BASE}/bronze/players"

CHECKPOINT_BASE  = f"{_BASE}/checkpoints/bronze"
TRIGGER_AVAILABLE_NOW = True  # serverless only supports AvailableNow/Once, not ProcessingTime


# ── Schemas ────────────────────────────────────────────────────────────────────
GAME_EVENT_SCHEMA = StructType([
    StructField("event_id",               StringType(),  True),
    StructField("game_id",                LongType(),    True),
    StructField("season",                 IntegerType(), True),
    StructField("game_type",              IntegerType(), True),
    StructField("event_type",             StringType(),  True),
    StructField("period",                 IntegerType(), True),
    StructField("period_type",            StringType(),  True),
    StructField("time_in_period",         StringType(),  True),
    StructField("time_remaining",         StringType(),  True),
    StructField("home_team_id",           IntegerType(), True),
    StructField("home_team_abbrev",       StringType(),  True),
    StructField("away_team_id",           IntegerType(), True),
    StructField("away_team_abbrev",       StringType(),  True),
    StructField("home_score",             IntegerType(), True),
    StructField("away_score",             IntegerType(), True),
    StructField("scoring_player_id",      LongType(),    True),
    StructField("scoring_player_total",   IntegerType(), True),
    StructField("assist1_player_id",      LongType(),    True),
    StructField("assist1_player_total",   IntegerType(), True),
    StructField("assist2_player_id",      LongType(),    True),
    StructField("assist2_player_total",   IntegerType(), True),
    StructField("goalie_in_net_id",       LongType(),    True),
    StructField("shot_type",              StringType(),  True),
    StructField("goal_type",              StringType(),  True),
    StructField("committed_by_player_id", LongType(),    True),
    StructField("drawn_by_player_id",     LongType(),    True),
    StructField("penalty_type",           StringType(),  True),
    StructField("duration_minutes",       IntegerType(), True),
    StructField("x_coord",               DoubleType(),   True),
    StructField("y_coord",               DoubleType(),   True),
    StructField("zone_code",              StringType(),  True),
    StructField("hittee_player_id",       LongType(),    True),
    StructField("hitting_player_id",      LongType(),    True),
    StructField("player_id",              LongType(),    True),
    StructField("ingested_at",            StringType(),  True),
    StructField("source",                 StringType(),  True),
])

PLAYER_STAT_SCHEMA = StructType([
    StructField("stat_id",            StringType(),  True),
    StructField("game_id",            LongType(),    True),
    StructField("season",             IntegerType(), True),
    StructField("team_id",            IntegerType(), True),
    StructField("team_abbrev",        StringType(),  True),
    StructField("player_id",          LongType(),    True),
    StructField("player_name",        StringType(),  True),
    StructField("position",           StringType(),  True),
    StructField("sweater_number",     IntegerType(), True),
    StructField("goals",              IntegerType(), True),
    StructField("assists",            IntegerType(), True),
    StructField("points",             IntegerType(), True),
    StructField("plus_minus",         IntegerType(), True),
    StructField("pim",                IntegerType(), True),
    StructField("hits",               IntegerType(), True),
    StructField("blocked_shots",      IntegerType(), True),
    StructField("shots",              IntegerType(), True),
    StructField("faceoff_wins",       IntegerType(), True),
    StructField("faceoff_attempts",   IntegerType(), True),
    StructField("saves",              IntegerType(), True),
    StructField("shots_against",      IntegerType(), True),
    StructField("save_pct",           DoubleType(),  True),
    StructField("goals_against",      IntegerType(), True),
    StructField("time_on_ice",        StringType(),  True),
    StructField("power_play_toi",     StringType(),  True),
    StructField("short_handed_toi",   StringType(),  True),
    StructField("giveaways",          IntegerType(), True),
    StructField("takeaways",          IntegerType(), True),
    StructField("ingested_at",        StringType(),  True),
    StructField("source",             StringType(),  True),
])

GAME_STATE_SCHEMA = StructType([
    StructField("game_id",          LongType(),    True),
    StructField("home_team_abbrev", StringType(),  True),
    StructField("away_team_abbrev", StringType(),  True),
    StructField("home_score",       IntegerType(), True),
    StructField("away_score",       IntegerType(), True),
    StructField("period",           IntegerType(), True),
    StructField("period_type",      StringType(),  True),
    StructField("time_remaining",   StringType(),  True),
    StructField("game_state",       StringType(),  True),
    StructField("in_intermission",  BooleanType(), True),
    StructField("clock_running",    BooleanType(), True),
    StructField("updated_at",       StringType(),  True),
])

PLAYER_SCHEMA = StructType([
    StructField("player_id",  LongType(),   True),
    StructField("full_name",  StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name",  StringType(), True),
    StructField("position",   StringType(), True),
    StructField("team_id",    IntegerType(), True),
])


# ── Inline upsert (avoids cloudpickle re-importing src.utils.delta in workers) ─
def _upsert(batch_df, path, merge_keys, update_cols=None):
    from delta.tables import DeltaTable as _DT
    ss = batch_df.sparkSession
    if not _DT.isDeltaTable(ss, path):
        batch_df.write.format("delta").mode("append").save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    merger = _DT.forPath(ss, path).alias("t").merge(batch_df.alias("s"), condition)
    if update_cols:
        merger = merger.whenMatchedUpdate(set={c: f"s.{c}" for c in update_cols})
    else:
        merger = merger.whenMatchedUpdateAll()
    merger.whenNotMatchedInsertAll().execute()


# ── Auto Loader helper ─────────────────────────────────────────────────────────
def _autoloader_stream(path: str, schema: StructType, file_format: str = "json"):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schemas/{path.split('/')[-1]}")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.useNotifications", "false")
        .schema(schema)
        .load(path)
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )


# ── foreachBatch writers ───────────────────────────────────────────────────────
_EVENT_UPDATE_COLS = [
    "event_type", "period", "period_type", "time_in_period", "time_remaining",
    "home_score", "away_score", "scoring_player_id", "scoring_player_total",
    "assist1_player_id", "assist1_player_total", "assist2_player_id",
    "assist2_player_total", "goalie_in_net_id", "shot_type", "goal_type",
    "committed_by_player_id", "drawn_by_player_id", "penalty_type",
    "duration_minutes", "x_coord", "y_coord", "zone_code",
]


def write_events_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    _upsert(batch_df, BRONZE_EVENTS, ["event_id", "game_id"], _EVENT_UPDATE_COLS)
    log.info("Bronze events batch %d: %d rows → Delta", epoch_id, batch_df.count())


def write_stats_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    _upsert(batch_df, BRONZE_STATS, ["stat_id"])
    log.info("Bronze stats batch %d: %d rows → Delta", epoch_id, batch_df.count())


def write_game_state_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    # Keep only the latest state per game
    latest = (
        batch_df
        .withColumn("_rn", F.row_number().over(
            __import__("pyspark.sql", fromlist=["Window"]).Window
            .partitionBy("game_id")
            .orderBy(F.col("updated_at").desc())
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    _upsert(latest, GAME_SCORES, ["game_id"])
    log.info("Game state batch %d → Delta", epoch_id)


def write_players_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    _upsert(batch_df, PLAYERS, ["player_id"],
            ["full_name", "first_name", "last_name", "position", "team_id"])
    log.info("Players batch %d: %d rows → Delta", epoch_id, batch_df.count())


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Bronze Auto Loader streaming → Delta …")

    ensure_schema(spark)

    events_q = (
        _autoloader_stream(LANDING_EVENTS, GAME_EVENT_SCHEMA)
        .writeStream
        .foreachBatch(write_events_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/game_events")
        .trigger(availableNow=True)
        .queryName("bronze_game_events")
        .start()
    )
    log.info("Bronze events stream: %s", events_q.id)

    stats_q = (
        _autoloader_stream(LANDING_STATS, PLAYER_STAT_SCHEMA)
        .writeStream
        .foreachBatch(write_stats_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/player_stats")
        .trigger(availableNow=True)
        .queryName("bronze_player_stats")
        .start()
    )
    log.info("Bronze stats stream: %s", stats_q.id)

    state_q = (
        _autoloader_stream(LANDING_STATE, GAME_STATE_SCHEMA, file_format="json")
        .writeStream
        .foreachBatch(write_game_state_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/game_state")
        .trigger(availableNow=True)
        .queryName("bronze_game_state")
        .start()
    )
    log.info("Game state stream: %s", state_q.id)

    players_q = (
        _autoloader_stream(LANDING_PLAYERS, PLAYER_SCHEMA)
        .writeStream
        .foreachBatch(write_players_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/players")
        .trigger(availableNow=True)
        .queryName("bronze_players")
        .start()
    )
    log.info("Players stream: %s", players_q.id)

    for q in [events_q, stats_q, state_q, players_q]:
        q.awaitTermination()

    # Register tables in metastore for Databricks SQL access
    for name, path in [
        ("bronze_game_events", BRONZE_EVENTS),
        ("bronze_player_stats", BRONZE_STATS),
        ("game_scores",         GAME_SCORES),
        ("players",             PLAYERS),
    ]:
        register_table(spark, name, path)


main()
