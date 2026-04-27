"""
silver_streaming.py
Reads bronze Delta tables as streams → validates, deduplicates, enriches
→ writes to silver Delta tables via MERGE.
Runs as a continuous Databricks Job task.
"""

import logging
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

sys.path.insert(0, "/Workspace/Repos/nhl-pipeline")
from src.utils.delta import (
    DB_CATALOG, DB_SCHEMA,
    ensure_schema, merge_into_delta, register_table,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("silver_streaming")

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE           = os.getenv("DELTA_BASE", "dbfs:/mnt/nhl")
BRONZE_EVENTS   = f"{_BASE}/bronze/game_events"
BRONZE_STATS    = f"{_BASE}/bronze/player_stats"
SILVER_EVENTS   = f"{_BASE}/silver/game_events"
SILVER_STATS    = f"{_BASE}/silver/player_stats"
CHECKPOINT_BASE = f"{_BASE}/checkpoints/silver"
TRIGGER_SECS    = 15
WATERMARK_DELAY = "5 minutes"


# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_mm_ss(col):
    parts = F.split(col, ":")
    return parts[0].cast(IntegerType()) * 60 + parts[1].cast(IntegerType())


# ── Transformations ────────────────────────────────────────────────────────────
def transform_events(df):
    valid = df.filter(
        F.col("event_id").isNotNull()
        & F.col("game_id").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("period").isNotNull()
        & F.col("period").between(1, 9)
        & F.col("bronze_ingested_at").isNotNull()
    )

    deduped = (
        valid
        .withWatermark("bronze_ingested_at", WATERMARK_DELAY)
        .dropDuplicates(["event_id", "event_type"])
    )

    return (
        deduped
        .withColumn("time_in_period_secs", parse_mm_ss(F.col("time_in_period")))
        .withColumn("time_remaining_secs", parse_mm_ss(F.col("time_remaining")))
        .withColumn("game_second",
            (F.col("period") - 1) * 1200 + F.col("time_in_period_secs"))
        .withColumn("score_differential",
            F.col("home_score") - F.col("away_score"))
        .withColumn("is_penalty_shot",
            F.col("goal_type").isin("penalty-shot"))
        .withColumn("is_overtime",
            F.col("period") > 3)
        .withColumn("silver_processed_at", F.current_timestamp())
    )


def transform_stats(df):
    valid = df.filter(
        F.col("stat_id").isNotNull()
        & F.col("game_id").isNotNull()
        & F.col("player_id").isNotNull()
    )

    deduped = (
        valid
        .withWatermark("bronze_ingested_at", WATERMARK_DELAY)
        .dropDuplicates(["stat_id"])
    )

    return (
        deduped
        .withColumn(
            "faceoff_pct",
            F.when(
                F.col("faceoff_attempts") > 0,
                F.round(F.col("faceoff_wins") / F.col("faceoff_attempts") * 100, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("toi_seconds", parse_mm_ss(F.col("time_on_ice")))
        .withColumn("silver_processed_at", F.current_timestamp())
    )


# ── foreachBatch writers ───────────────────────────────────────────────────────
_EVENT_SILVER_COLS = [
    "event_id", "game_id", "season", "game_type", "event_type",
    "period", "period_type", "time_in_period", "time_remaining",
    "time_in_period_secs", "time_remaining_secs", "game_second",
    "home_team_id", "home_team_abbrev", "away_team_id", "away_team_abbrev",
    "home_score", "away_score", "score_differential", "zone_code",
    "x_coord", "y_coord",
    "scoring_player_id", "scoring_player_total",
    "assist1_player_id", "assist1_player_total",
    "assist2_player_id", "assist2_player_total",
    "goalie_in_net_id", "shot_type", "goal_type", "penalty_type",
    "duration_minutes", "committed_by_player_id", "drawn_by_player_id",
    "hittee_player_id", "hitting_player_id", "player_id",
    "is_overtime", "is_penalty_shot", "silver_processed_at",
]

_EVENT_UPDATE_COLS = [
    "event_type", "period", "period_type", "time_in_period", "time_remaining",
    "time_in_period_secs", "time_remaining_secs", "game_second",
    "home_score", "away_score", "score_differential", "zone_code",
    "x_coord", "y_coord", "scoring_player_id", "scoring_player_total",
    "assist1_player_id", "assist1_player_total", "assist2_player_id",
    "assist2_player_total", "goalie_in_net_id", "shot_type", "goal_type",
    "penalty_type", "duration_minutes", "committed_by_player_id",
    "drawn_by_player_id", "is_overtime", "is_penalty_shot",
    "silver_processed_at",
]


def write_events_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    out = batch_df.select(*[c for c in _EVENT_SILVER_COLS if c in batch_df.columns])
    merge_into_delta(
        spark, SILVER_EVENTS, out,
        merge_keys=["event_id", "game_id"],
        update_cols=_EVENT_UPDATE_COLS,
    )
    log.info("Silver events batch %d → Delta", epoch_id)


def write_stats_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    merge_into_delta(
        spark, SILVER_STATS, batch_df,
        merge_keys=["stat_id"],
        update_cols=None,
    )
    log.info("Silver stats batch %d → Delta", epoch_id)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Silver streaming layer → Delta …")

    ensure_schema(spark)

    bronze_events_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE_EVENTS)
    )
    silver_events = transform_events(bronze_events_stream)
    eq = (
        silver_events.writeStream
        .foreachBatch(write_events_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/game_events")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .queryName("silver_game_events")
        .start()
    )
    log.info("Silver events stream: %s", eq.id)

    bronze_stats_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(BRONZE_STATS)
    )
    silver_stats = transform_stats(bronze_stats_stream)
    sq = (
        silver_stats.writeStream
        .foreachBatch(write_stats_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/player_stats")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .queryName("silver_player_stats")
        .start()
    )
    log.info("Silver stats stream: %s", sq.id)

    for name, path in [
        ("silver_game_events", SILVER_EVENTS),
        ("silver_player_stats", SILVER_STATS),
    ]:
        register_table(spark, name, path)

    spark.streams.awaitAnyTermination()


main()
