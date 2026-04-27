"""
gold_streaming.py
Reads silver Delta tables as streams → aggregates → writes to gold Delta tables.
Runs as a continuous Databricks Job task.
"""

import logging
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/Workspace/Repos/nhl-pipeline")
from src.utils.delta import (
    ensure_schema, merge_into_delta, register_table,
)

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("gold_streaming")

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE            = os.getenv("DELTA_BASE", "dbfs:/mnt/nhl")
SILVER_EVENTS    = f"{_BASE}/silver/game_events"
GOLD_SCORING     = f"{_BASE}/gold/player_scoring"
GOLD_MOMENTUM    = f"{_BASE}/gold/team_momentum"
GOLD_PENALTIES   = f"{_BASE}/gold/penalty_summary"
CHECKPOINT_BASE  = f"{_BASE}/checkpoints/gold"
TRIGGER_SECS     = 30
WATERMARK_DELAY  = "10 minutes"


# ── Stream builder ─────────────────────────────────────────────────────────────
def _silver_events_stream():
    return (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER_EVENTS)
    )


# ── Gold 1: player scoring ─────────────────────────────────────────────────────
def build_player_scoring(stream):
    goals = (
        stream
        .filter(F.col("event_type") == "goal")
        .withWatermark("silver_processed_at", WATERMARK_DELAY)
    )

    scorers = goals.filter(F.col("scoring_player_id").isNotNull()).select(
        "game_id", "silver_processed_at",
        F.col("scoring_player_id").alias("player_id"),
        F.lit(1).alias("is_goal"),
        F.lit(0).alias("is_primary_assist"),
        F.lit(0).alias("is_secondary_assist"),
    )
    a1 = goals.filter(F.col("assist1_player_id").isNotNull()).select(
        "game_id", "silver_processed_at",
        F.col("assist1_player_id").alias("player_id"),
        F.lit(0).alias("is_goal"),
        F.lit(1).alias("is_primary_assist"),
        F.lit(0).alias("is_secondary_assist"),
    )
    a2 = goals.filter(F.col("assist2_player_id").isNotNull()).select(
        "game_id", "silver_processed_at",
        F.col("assist2_player_id").alias("player_id"),
        F.lit(0).alias("is_goal"),
        F.lit(0).alias("is_primary_assist"),
        F.lit(1).alias("is_secondary_assist"),
    )

    return (
        scorers.union(a1).union(a2)
        .groupBy("game_id", "player_id")
        .agg(
            F.sum("is_goal").alias("goals"),
            F.sum("is_primary_assist").alias("primary_assists"),
            F.sum("is_secondary_assist").alias("secondary_assists"),
            F.max("silver_processed_at").alias("last_event_at"),
        )
        .withColumn("total_assists", F.col("primary_assists") + F.col("secondary_assists"))
        .withColumn("points", F.col("goals") + F.col("total_assists"))
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def write_scoring_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    merge_into_delta(
        spark, GOLD_SCORING, batch_df,
        merge_keys=["game_id", "player_id"],
        update_cols=["goals", "primary_assists", "secondary_assists",
                     "total_assists", "points", "last_event_at", "gold_updated_at"],
    )
    log.info("Gold scoring batch %d → Delta", epoch_id)


# ── Gold 2: team momentum ──────────────────────────────────────────────────────
def build_team_momentum(stream):
    goal_events = (
        stream
        .filter(F.col("event_type") == "goal")
        .withWatermark("silver_processed_at", WATERMARK_DELAY)
    )

    home_goals = goal_events.groupBy(
        "game_id",
        F.col("home_team_abbrev").alias("team_abbrev"),
        F.window("silver_processed_at", "5 minutes", "1 minute").alias("event_window"),
    ).agg(F.count("*").alias("goals_in_window"))

    away_goals = goal_events.groupBy(
        "game_id",
        F.col("away_team_abbrev").alias("team_abbrev"),
        F.window("silver_processed_at", "5 minutes", "1 minute").alias("event_window"),
    ).agg(F.count("*").alias("goals_in_window"))

    return (
        home_goals.union(away_goals)
        .withColumn("window_start", F.col("event_window.start"))
        .withColumn("window_end",   F.col("event_window.end"))
        .drop("event_window")
        .withColumnRenamed("goals_in_window", "momentum_score")
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def write_momentum_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    merge_into_delta(
        spark, GOLD_MOMENTUM, batch_df,
        merge_keys=["game_id", "team_abbrev", "window_start"],
        update_cols=["window_end", "momentum_score", "gold_updated_at"],
    )
    log.info("Gold momentum batch %d → Delta", epoch_id)


# ── Gold 3: penalty summary ────────────────────────────────────────────────────
def build_penalty_summary(stream):
    return (
        stream
        .filter(F.col("event_type") == "penalty")
        .withWatermark("silver_processed_at", WATERMARK_DELAY)
        .groupBy("game_id", "penalty_type")
        .agg(
            F.count("*").alias("penalty_count"),
            F.sum("duration_minutes").alias("total_penalty_minutes"),
            F.approx_count_distinct("committed_by_player_id").alias("unique_offenders"),
            F.max("silver_processed_at").alias("last_penalty_at"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def write_penalty_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    merge_into_delta(
        spark, GOLD_PENALTIES, batch_df,
        merge_keys=["game_id", "penalty_type"],
        update_cols=["penalty_count", "total_penalty_minutes",
                     "unique_offenders", "last_penalty_at", "gold_updated_at"],
    )
    log.info("Gold penalty batch %d → Delta", epoch_id)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Gold streaming layer → Delta …")

    ensure_schema(spark)

    scoring_q = (
        build_player_scoring(_silver_events_stream())
        .writeStream
        .foreachBatch(write_scoring_batch)
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/player_scoring")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .queryName("gold_player_scoring")
        .start()
    )
    log.info("Gold player-scoring stream: %s", scoring_q.id)

    momentum_q = (
        build_team_momentum(_silver_events_stream())
        .writeStream
        .foreachBatch(write_momentum_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/team_momentum")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .queryName("gold_team_momentum")
        .start()
    )
    log.info("Gold team-momentum stream: %s", momentum_q.id)

    penalty_q = (
        build_penalty_summary(_silver_events_stream())
        .writeStream
        .foreachBatch(write_penalty_batch)
        .outputMode("complete")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/penalty_summary")
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .queryName("gold_penalty_summary")
        .start()
    )
    log.info("Gold penalty-summary stream: %s", penalty_q.id)

    for name, path in [
        ("gold_player_scoring",  GOLD_SCORING),
        ("gold_team_momentum",   GOLD_MOMENTUM),
        ("gold_penalty_summary", GOLD_PENALTIES),
    ]:
        register_table(spark, name, path)

    spark.streams.awaitAnyTermination()


main()
