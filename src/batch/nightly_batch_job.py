"""
nightly_batch_job.py
Scheduled nightly (04:00 UTC) via Databricks Workflows.
Aggregates daily game data → updates historical Delta tables.

Usage (from databricks.yml):
  spark_python_task with parameters: ["--seasons", "20242025"]
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

_f = globals().get("__file__") or sys._getframe(0).f_code.co_filename
sys.path.insert(0, str(Path(_f).parents[2]))
from src.utils.delta import ensure_schema, register_table

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("nightly_batch")

# ── Paths ──────────────────────────────────────────────────────────────────────
_BASE = os.getenv("DELTA_BASE") or f"/Volumes/{spark.catalog.currentCatalog()}/nhl/nhl_data"
SILVER_EVENTS       = f"{_BASE}/silver/game_events"
SILVER_STATS        = f"{_BASE}/silver/player_stats"
HIST_PLAYER_SEASON  = f"{_BASE}/historical/player_season_stats"
HIST_TEAM_SEASON    = f"{_BASE}/historical/team_season_stats"
HIST_GAME_SUMMARY   = f"{_BASE}/historical/game_summaries"
HIST_PLAYOFF_SERIES = f"{_BASE}/historical/playoff_series"


# ── CLI args ───────────────────────────────────────────────────────────────────
def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=str(date.today() - timedelta(days=1)),
        help="Processing date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--seasons",
        default=None,
        help="Comma-separated season codes, e.g. 20242025 (overrides --date filtering)",
    )
    return parser.parse_known_args()[0]


# ── Silver readers ─────────────────────────────────────────────────────────────
def read_silver_events(proc_date: str):
    return (
        spark.read.format("delta").load(SILVER_EVENTS)
        .filter(F.to_date("silver_processed_at") == proc_date)
    )


def read_silver_stats(proc_date: str):
    return (
        spark.read.format("delta").load(SILVER_STATS)
        .filter(F.to_date("silver_processed_at") == proc_date)
    )


# ── 1. Game summaries ──────────────────────────────────────────────────────────
def compute_game_summaries(silver_events, proc_date: str):
    goals = silver_events.filter(F.col("event_type") == "goal")

    final_scores = (
        goals
        .groupBy("game_id", "season", "game_type", "home_team_abbrev", "away_team_abbrev")
        .agg(
            F.last("home_score", ignorenulls=True).alias("final_home_score"),
            F.last("away_score", ignorenulls=True).alias("final_away_score"),
            F.max("period").alias("periods_played"),
            F.count("*").alias("total_goals"),
        )
    )

    shots = (
        silver_events
        .filter(F.col("event_type").isin("goal", "shot-on-goal"))
        .groupBy("game_id")
        .agg(F.count("*").alias("total_shots_on_goal"))
    )

    penalties = (
        silver_events
        .filter(F.col("event_type") == "penalty")
        .groupBy("game_id")
        .agg(
            F.count("*").alias("total_penalties"),
            F.sum("duration_minutes").alias("total_penalty_minutes"),
        )
    )

    return (
        final_scores
        .join(shots,     on="game_id", how="left")
        .join(penalties, on="game_id", how="left")
        .withColumn("game_date", F.lit(proc_date).cast("date"))
        .withColumn(
            "winner",
            F.when(
                F.col("final_home_score") > F.col("final_away_score"),
                F.col("home_team_abbrev"),
            ).otherwise(F.col("away_team_abbrev")),
        )
        .withColumn("went_to_overtime", F.col("periods_played") > 3)
        .withColumn("batch_processed_at", F.current_timestamp())
    )


# ── 2. Player season-to-date stats ─────────────────────────────────────────────
def compute_player_season_stats(silver_stats, proc_date: str):
    today = (
        silver_stats
        .groupBy("player_id", "player_name", "team_abbrev", "position", "season")
        .agg(
            F.countDistinct("game_id").alias("gp_today"),
            F.sum("goals").alias("goals_today"),
            F.sum("assists").alias("assists_today"),
            F.sum("points").alias("points_today"),
            F.sum("plus_minus").alias("plus_minus_today"),
            F.sum("pim").alias("pim_today"),
            F.sum("shots").alias("shots_today"),
            F.sum("hits").alias("hits_today"),
            F.sum("blocked_shots").alias("blocked_shots_today"),
            F.sum("toi_seconds").alias("toi_seconds_today"),
        )
        .withColumn("last_game_date", F.lit(proc_date).cast("date"))
    )

    if not DeltaTable.isDeltaTable(spark, HIST_PLAYER_SEASON):
        log.info("Creating player season stats Delta table …")
        (
            today
            .withColumnRenamed("gp_today",           "games_played")
            .withColumnRenamed("goals_today",         "goals")
            .withColumnRenamed("assists_today",       "assists")
            .withColumnRenamed("points_today",        "points")
            .withColumnRenamed("plus_minus_today",    "plus_minus")
            .withColumnRenamed("pim_today",           "pim")
            .withColumnRenamed("shots_today",         "shots")
            .withColumnRenamed("hits_today",          "hits")
            .withColumnRenamed("blocked_shots_today", "blocked_shots")
            .withColumnRenamed("toi_seconds_today",   "toi_seconds")
            .write.format("delta").mode("overwrite").save(HIST_PLAYER_SEASON)
        )
        return

    DeltaTable.forPath(spark, HIST_PLAYER_SEASON).alias("h").merge(
        today.alias("t"),
        "h.player_id = t.player_id AND h.season = t.season",
    ).whenMatchedUpdate(set={
        "games_played":  "h.games_played  + t.gp_today",
        "goals":         "h.goals         + t.goals_today",
        "assists":       "h.assists       + t.assists_today",
        "points":        "h.points        + t.points_today",
        "plus_minus":    "h.plus_minus    + t.plus_minus_today",
        "pim":           "h.pim           + t.pim_today",
        "shots":         "h.shots         + t.shots_today",
        "hits":          "h.hits          + t.hits_today",
        "blocked_shots": "h.blocked_shots + t.blocked_shots_today",
        "toi_seconds":   "h.toi_seconds   + t.toi_seconds_today",
        "last_game_date": "t.last_game_date",
    }).whenNotMatchedInsert(values={
        "player_id":     "t.player_id",
        "player_name":   "t.player_name",
        "team_abbrev":   "t.team_abbrev",
        "position":      "t.position",
        "season":        "t.season",
        "games_played":  "t.gp_today",
        "goals":         "t.goals_today",
        "assists":       "t.assists_today",
        "points":        "t.points_today",
        "plus_minus":    "t.plus_minus_today",
        "pim":           "t.pim_today",
        "shots":         "t.shots_today",
        "hits":          "t.hits_today",
        "blocked_shots": "t.blocked_shots_today",
        "toi_seconds":   "t.toi_seconds_today",
        "last_game_date": "t.last_game_date",
    }).execute()

    log.info("Player season stats merged for %s.", proc_date)


# ── 3. Team season-to-date stats ───────────────────────────────────────────────
def compute_team_season_stats(game_summaries):
    home = game_summaries.select(
        F.col("home_team_abbrev").alias("team_abbrev"),
        F.col("season"),
        F.when(F.col("final_home_score") > F.col("final_away_score"), 1).otherwise(0).alias("wins"),
        F.when(F.col("final_home_score") < F.col("final_away_score"), 1).otherwise(0).alias("losses"),
        F.col("final_home_score").alias("goals_for"),
        F.col("final_away_score").alias("goals_against"),
        F.lit(1).alias("gp"),
    )
    away = game_summaries.select(
        F.col("away_team_abbrev").alias("team_abbrev"),
        F.col("season"),
        F.when(F.col("final_away_score") > F.col("final_home_score"), 1).otherwise(0).alias("wins"),
        F.when(F.col("final_away_score") < F.col("final_home_score"), 1).otherwise(0).alias("losses"),
        F.col("final_away_score").alias("goals_for"),
        F.col("final_home_score").alias("goals_against"),
        F.lit(1).alias("gp"),
    )

    today = (
        home.union(away)
        .groupBy("team_abbrev", "season")
        .agg(
            F.sum("gp").alias("gp_today"),
            F.sum("wins").alias("wins_today"),
            F.sum("losses").alias("losses_today"),
            F.sum("goals_for").alias("gf_today"),
            F.sum("goals_against").alias("ga_today"),
        )
    )

    if not DeltaTable.isDeltaTable(spark, HIST_TEAM_SEASON):
        today.write.format("delta").mode("overwrite").save(HIST_TEAM_SEASON)
        return

    DeltaTable.forPath(spark, HIST_TEAM_SEASON).alias("h").merge(
        today.alias("t"),
        "h.team_abbrev = t.team_abbrev AND h.season = t.season",
    ).whenMatchedUpdate(set={
        "games_played":  "h.games_played  + t.gp_today",
        "wins":          "h.wins          + t.wins_today",
        "losses":        "h.losses        + t.losses_today",
        "goals_for":     "h.goals_for     + t.gf_today",
        "goals_against": "h.goals_against + t.ga_today",
    }).whenNotMatchedInsert(values={
        "team_abbrev":   "t.team_abbrev",
        "season":        "t.season",
        "games_played":  "t.gp_today",
        "wins":          "t.wins_today",
        "losses":        "t.losses_today",
        "goals_for":     "t.gf_today",
        "goals_against": "t.ga_today",
    }).execute()

    log.info("Team season stats merged.")


# ── 4. Playoff series tracker ──────────────────────────────────────────────────
def compute_playoff_series(game_summaries):
    series = (
        game_summaries
        .withColumn("team_a", F.least("home_team_abbrev", "away_team_abbrev"))
        .withColumn("team_b", F.greatest("home_team_abbrev", "away_team_abbrev"))
        .withColumn("team_a_wins", F.when(F.col("winner") == F.col("team_a"), 1).otherwise(0))
        .withColumn("team_b_wins", F.when(F.col("winner") == F.col("team_b"), 1).otherwise(0))
        .groupBy("team_a", "team_b", "season")
        .agg(
            F.sum("team_a_wins").alias("wins_today_a"),
            F.sum("team_b_wins").alias("wins_today_b"),
            F.count("*").alias("games_played_today"),
        )
    )

    if not DeltaTable.isDeltaTable(spark, HIST_PLAYOFF_SERIES):
        series.write.format("delta").mode("overwrite").save(HIST_PLAYOFF_SERIES)
        return

    DeltaTable.forPath(spark, HIST_PLAYOFF_SERIES).alias("h").merge(
        series.alias("t"),
        "h.team_a = t.team_a AND h.team_b = t.team_b AND h.season = t.season",
    ).whenMatchedUpdate(set={
        "team_a_wins":  "h.team_a_wins  + t.wins_today_a",
        "team_b_wins":  "h.team_b_wins  + t.wins_today_b",
        "games_played": "h.games_played + t.games_played_today",
    }).whenNotMatchedInsert(values={
        "team_a":        "t.team_a",
        "team_b":        "t.team_b",
        "season":        "t.season",
        "team_a_wins":   "t.wins_today_a",
        "team_b_wins":   "t.wins_today_b",
        "games_played":  "t.games_played_today",
    }).execute()

    log.info("Playoff series stats merged.")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    args = _parse_args()
    proc_date = args.date
    log.info("Nightly batch running for date: %s", proc_date)

    ensure_schema(spark)

    silver_events = read_silver_events(proc_date)
    silver_stats  = read_silver_stats(proc_date)

    if silver_events.rdd.isEmpty():
        log.warning("No silver event data for %s — nothing to do.", proc_date)
        return

    game_summaries = compute_game_summaries(silver_events, proc_date)
    (
        game_summaries.write
        .format("delta")
        .mode("append")
        .partitionBy("game_date")
        .save(HIST_GAME_SUMMARY)
    )
    log.info("Game summaries written.")

    compute_player_season_stats(silver_stats, proc_date)
    compute_team_season_stats(game_summaries)

    playoff_games = game_summaries.filter(F.col("game_type") == 3)
    if not playoff_games.rdd.isEmpty():
        compute_playoff_series(playoff_games)

    for name, path in [
        ("hist_player_season_stats", HIST_PLAYER_SEASON),
        ("hist_team_season_stats",   HIST_TEAM_SEASON),
        ("hist_game_summaries",      HIST_GAME_SUMMARY),
        ("hist_playoff_series",      HIST_PLAYOFF_SERIES),
    ]:
        register_table(spark, name, path)

    log.info("Nightly batch complete for %s.", proc_date)


main()
