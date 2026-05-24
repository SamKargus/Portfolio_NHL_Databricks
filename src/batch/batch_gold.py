"""
Gold layer — aggregated player and team statistics derived from nhl.silver.nhl_plays.

Produces two tables:
  nhl.gold.player_stats  — per-player, per-season, per-game-type aggregates
                           (goals, assists, points, shots, hits, faceoffs, PIM, …)
  nhl.gold.team_stats    — per-team, per-season, per-game-type aggregates
                           (wins, losses, goals, shots, PP goals, SH goals, PIM, …)

Both tables are fully overwritten on each run so they always reflect the current
silver data.  The script is idempotent and safe to re-run at any time.
"""

import logging
from pathlib import Path
from datetime import datetime
from pyspark.sql import functions as F

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

SILVER_TABLE       = "nhl.silver.nhl_plays"
PLAYER_STATS_TABLE = "nhl.gold.player_stats"
TEAM_STATS_TABLE   = "nhl.gold.team_stats"

# situation_code layout: [away_goalie][away_skaters][home_skaters][home_goalie]
# e.g. "1551" = even strength 5v5 with both goalies on ice
_SC_AWAY_SK = F.col("situation_code").substr(2, 1).cast("int")
_SC_HOME_SK = F.col("situation_code").substr(3, 1).cast("int")


# ----------------------------------------------------------------------
# Player stats
# ----------------------------------------------------------------------
def build_player_stats(silver):
    """
    Returns a DataFrame with one row per (season, game_type, player_id).

    Columns:
      season, game_type, player_id,
      games_played, goals, primary_assists, secondary_assists, assists, points,
      shots_on_goal, shooting_pct,
      hits, blocked_shots,
      faceoff_wins, faceoff_losses, faceoff_pct,
      penalty_minutes, giveaways, takeaways,
      ingestion_timestamp
    """
    GC = ["season", "game_type"]  # group-by columns shared across all aggregations

    # ---- games_played ------------------------------------------------
    # Union every column that carries a player ID; a player "played" in any
    # game where their ID appears in any event role.
    player_id_cols = [
        "scoring_player_id",
        "assist1_player_id", "assist2_player_id", "assist3_player_id",
        "shooting_player_id", "goalie_in_net_id",
        "hitting_player_id", "hittee_player_id",
        "blocking_player_id",
        "winning_player_id", "losing_player_id",
        "committed_by_player_id", "drawn_by_player_id", "served_by_player_id",
        "player_id",
    ]
    appearances = None
    for col_name in player_id_cols:
        subset = (
            silver.filter(F.col(col_name).isNotNull())
            .select(*GC, "game_id", F.col(col_name).alias("pid"))
        )
        appearances = subset if appearances is None else appearances.union(subset)

    games_played = (
        appearances.distinct()
        .groupBy(*GC, "pid")
        .agg(F.countDistinct("game_id").alias("games_played"))
        .withColumnRenamed("pid", "player_id")
    )

    # ---- goals -------------------------------------------------------
    goals = (
        silver.filter(F.col("type_desc_key") == "goal")
        .filter(F.col("scoring_player_id").isNotNull())
        .groupBy(*GC, F.col("scoring_player_id").alias("player_id"))
        .agg(F.count("*").alias("goals"))
    )

    # ---- primary assists ---------------------------------------------
    primary_assists = (
        silver.filter(F.col("type_desc_key") == "goal")
        .filter(F.col("assist1_player_id").isNotNull())
        .groupBy(*GC, F.col("assist1_player_id").alias("player_id"))
        .agg(F.count("*").alias("primary_assists"))
    )

    # ---- secondary assists (assist2 + assist3 combined) --------------
    secondary_assists = (
        silver.filter(F.col("type_desc_key") == "goal")
        .select(*GC, "assist2_player_id", "assist3_player_id")
        .select(
            *GC,
            F.explode(
                F.array(F.col("assist2_player_id"), F.col("assist3_player_id"))
            ).alias("player_id"),
        )
        .filter(F.col("player_id").isNotNull())
        .groupBy(*GC, "player_id")
        .agg(F.count("*").alias("secondary_assists"))
    )

    # ---- shots on goal (goals count as shots) -----------------------
    shots_on_goal = (
        silver.filter(F.col("type_desc_key").isin("shot-on-goal", "goal"))
        .filter(F.col("shooting_player_id").isNotNull())
        .groupBy(*GC, F.col("shooting_player_id").alias("player_id"))
        .agg(F.count("*").alias("shots_on_goal"))
    )

    # ---- hits (the hitter, not the recipient) -----------------------
    hits = (
        silver.filter(F.col("type_desc_key") == "hit")
        .filter(F.col("hitting_player_id").isNotNull())
        .groupBy(*GC, F.col("hitting_player_id").alias("player_id"))
        .agg(F.count("*").alias("hits"))
    )

    # ---- blocked shots (the defender who blocked) ------------------
    blocked_shots = (
        silver.filter(F.col("type_desc_key") == "blocked-shot")
        .filter(F.col("blocking_player_id").isNotNull())
        .groupBy(*GC, F.col("blocking_player_id").alias("player_id"))
        .agg(F.count("*").alias("blocked_shots"))
    )

    # ---- faceoffs ---------------------------------------------------
    faceoff_wins = (
        silver.filter(F.col("type_desc_key") == "faceoff")
        .filter(F.col("winning_player_id").isNotNull())
        .groupBy(*GC, F.col("winning_player_id").alias("player_id"))
        .agg(F.count("*").alias("faceoff_wins"))
    )

    faceoff_losses = (
        silver.filter(F.col("type_desc_key") == "faceoff")
        .filter(F.col("losing_player_id").isNotNull())
        .groupBy(*GC, F.col("losing_player_id").alias("player_id"))
        .agg(F.count("*").alias("faceoff_losses"))
    )

    # ---- penalty minutes -------------------------------------------
    pim = (
        silver.filter(F.col("type_desc_key") == "penalty")
        .filter(F.col("committed_by_player_id").isNotNull())
        .filter(F.col("penalty_duration").isNotNull())
        .groupBy(*GC, F.col("committed_by_player_id").alias("player_id"))
        .agg(F.sum("penalty_duration").alias("penalty_minutes"))
    )

    # ---- giveaways & takeaways -------------------------------------
    giveaways = (
        silver.filter(F.col("type_desc_key") == "giveaway")
        .filter(F.col("player_id").isNotNull())
        .groupBy(*GC, "player_id")
        .agg(F.count("*").alias("giveaways"))
    )

    takeaways = (
        silver.filter(F.col("type_desc_key") == "takeaway")
        .filter(F.col("player_id").isNotNull())
        .groupBy(*GC, "player_id")
        .agg(F.count("*").alias("takeaways"))
    )

    # ---- join & derive computed columns ----------------------------
    z = F.lit(0).cast("bigint")  # default zero for nullable counts

    return (
        games_played
        .join(goals,            [*GC, "player_id"], "left")
        .join(primary_assists,  [*GC, "player_id"], "left")
        .join(secondary_assists,[*GC, "player_id"], "left")
        .join(shots_on_goal,    [*GC, "player_id"], "left")
        .join(hits,             [*GC, "player_id"], "left")
        .join(blocked_shots,    [*GC, "player_id"], "left")
        .join(faceoff_wins,     [*GC, "player_id"], "left")
        .join(faceoff_losses,   [*GC, "player_id"], "left")
        .join(pim,              [*GC, "player_id"], "left")
        .join(giveaways,        [*GC, "player_id"], "left")
        .join(takeaways,        [*GC, "player_id"], "left")
        .withColumn("goals",             F.coalesce("goals", z))
        .withColumn("primary_assists",   F.coalesce("primary_assists", z))
        .withColumn("secondary_assists", F.coalesce("secondary_assists", z))
        .withColumn("assists",           F.col("primary_assists") + F.col("secondary_assists"))
        .withColumn("points",            F.col("goals") + F.col("primary_assists") + F.col("secondary_assists"))
        .withColumn("shots_on_goal",     F.coalesce("shots_on_goal", z))
        .withColumn(
            "shooting_pct",
            F.when(
                F.col("shots_on_goal") > 0,
                F.round(F.col("goals").cast("double") / F.col("shots_on_goal") * 100, 2),
            ),
        )
        .withColumn("hits",              F.coalesce("hits", z))
        .withColumn("blocked_shots",     F.coalesce("blocked_shots", z))
        .withColumn("faceoff_wins",      F.coalesce("faceoff_wins", z))
        .withColumn("faceoff_losses",    F.coalesce("faceoff_losses", z))
        .withColumn(
            "faceoff_pct",
            F.when(
                F.col("faceoff_wins") + F.col("faceoff_losses") > 0,
                F.round(
                    F.col("faceoff_wins").cast("double")
                    / (F.col("faceoff_wins") + F.col("faceoff_losses"))
                    * 100,
                    2,
                ),
            ),
        )
        .withColumn("penalty_minutes",   F.coalesce("penalty_minutes", z))
        .withColumn("giveaways",         F.coalesce("giveaways", z))
        .withColumn("takeaways",         F.coalesce("takeaways", z))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "season", "game_type", "player_id",
            "games_played",
            "goals", "primary_assists", "secondary_assists", "assists", "points",
            "shots_on_goal", "shooting_pct",
            "hits", "blocked_shots",
            "faceoff_wins", "faceoff_losses", "faceoff_pct",
            "penalty_minutes", "giveaways", "takeaways",
            "ingestion_timestamp",
        )
    )


# ----------------------------------------------------------------------
# Team stats
# ----------------------------------------------------------------------
def build_team_stats(silver):
    """
    Returns a DataFrame with one row per (season, game_type, team_id).

    Columns:
      season, game_type, team_id, team_abbrev, team_name,
      games_played, wins, losses, ot_losses, standings_points, win_pct,
      goals_for, goals_against, goal_differential,
      shots_for, shots_against,
      power_play_goals, short_handed_goals, penalty_minutes,
      ingestion_timestamp
    """
    GC = ["season", "game_type"]

    # ---- one row per game -------------------------------------------
    # All plays in a game share the same game-context columns; pick one row
    # per game_id to avoid double-counting in the aggregation below.
    games = (
        silver
        .select(
            *GC, "game_id",
            "home_team_id", "home_team_abbrev", "home_team_name",
            "away_team_id", "away_team_abbrev", "away_team_name",
            "home_final_score", "away_final_score",
            "home_final_sog",   "away_final_sog",
            "last_period_type",
        )
        .dropDuplicates(["game_id"])
        .filter(F.col("home_final_score").isNotNull())
        .filter(F.col("away_final_score").isNotNull())
    )

    # ---- build home/away perspectives with win/loss flags -----------
    def _team_game_rows(games, side):
        """Return one row per game from the perspective of the given side ('home'/'away')."""
        opp = "away" if side == "home" else "home"
        scored_more = F.col(f"{side}_final_score") > F.col(f"{opp}_final_score")
        scored_less  = F.col(f"{side}_final_score") < F.col(f"{opp}_final_score")
        extra_time   = F.coalesce(F.col("last_period_type"), F.lit("REG")).isin("OT", "SO")

        return games.select(
            *GC, "game_id",
            F.col(f"{side}_team_id").alias("team_id"),
            F.col(f"{side}_team_abbrev").alias("team_abbrev"),
            F.col(f"{side}_team_name").alias("team_name"),
            F.col(f"{side}_final_score").alias("goals_for"),
            F.col(f"{opp}_final_score").alias("goals_against"),
            F.col(f"{side}_final_sog").alias("shots_for"),
            F.col(f"{opp}_final_sog").alias("shots_against"),
            F.when(scored_more, 1).otherwise(0).alias("win"),
            F.when(scored_less &  extra_time, 1).otherwise(0).alias("ot_loss"),
            F.when(scored_less & ~extra_time, 1).otherwise(0).alias("loss"),
        )

    all_game_rows = _team_game_rows(games, "home").union(_team_game_rows(games, "away"))

    base_stats = (
        all_game_rows
        .groupBy(*GC, "team_id", "team_abbrev", "team_name")
        .agg(
            F.count("game_id").alias("games_played"),
            F.sum("win").alias("wins"),
            F.sum("loss").alias("losses"),
            F.sum("ot_loss").alias("ot_losses"),
            F.sum("goals_for").alias("goals_for"),
            F.sum("goals_against").alias("goals_against"),
            F.sum(F.coalesce(F.col("shots_for"),   F.lit(0))).alias("shots_for"),
            F.sum(F.coalesce(F.col("shots_against"), F.lit(0))).alias("shots_against"),
        )
    )

    # ---- power-play and short-handed goals --------------------------
    # situation_code layout: [away_goalie][away_skaters][home_skaters][home_goalie]
    # PP goal = team scored while having more skaters on ice than the opponent.
    # SH goal = team scored while having fewer skaters on ice.
    goal_plays = (
        silver
        .filter(F.col("type_desc_key") == "goal")
        .filter(F.col("situation_code").isNotNull())
        .filter(F.length("situation_code") == 4)
        .filter(F.col("event_owner_team_id").isNotNull())
        .withColumn("away_sk", _SC_AWAY_SK)
        .withColumn("home_sk", _SC_HOME_SK)
    )

    def _special_goals(goal_plays, side, skater_cond):
        """Count goals for 'side' team (home/away) satisfying skater_cond (PP or SH)."""
        opp = "away" if side == "home" else "home"
        return (
            goal_plays
            .filter(F.col("event_owner_team_id") == F.col(f"{side}_team_id"))
            .filter(skater_cond(side, opp))
            .groupBy(*GC, F.col(f"{side}_team_id").alias("team_id"))
            .agg(F.count("*").alias("goals"))
        )

    pp_cond = lambda s, o: F.col(f"{s}_sk") > F.col(f"{o}_sk")
    sh_cond = lambda s, o: F.col(f"{s}_sk") < F.col(f"{o}_sk")

    pp_goals = (
        _special_goals(goal_plays, "home", pp_cond)
        .union(_special_goals(goal_plays, "away", pp_cond))
        .groupBy(*GC, "team_id")
        .agg(F.sum("goals").alias("power_play_goals"))
    )

    sh_goals = (
        _special_goals(goal_plays, "home", sh_cond)
        .union(_special_goals(goal_plays, "away", sh_cond))
        .groupBy(*GC, "team_id")
        .agg(F.sum("goals").alias("short_handed_goals"))
    )

    # ---- penalty minutes per team -----------------------------------
    team_pim = (
        silver
        .filter(F.col("type_desc_key") == "penalty")
        .filter(F.col("event_owner_team_id").isNotNull())
        .filter(F.col("penalty_duration").isNotNull())
        .groupBy(*GC, F.col("event_owner_team_id").alias("team_id"))
        .agg(F.sum("penalty_duration").alias("penalty_minutes"))
    )

    # ---- join & derive computed columns ----------------------------
    z = F.lit(0).cast("bigint")

    return (
        base_stats
        .join(pp_goals,  [*GC, "team_id"], "left")
        .join(sh_goals,  [*GC, "team_id"], "left")
        .join(team_pim,  [*GC, "team_id"], "left")
        .withColumn("power_play_goals",   F.coalesce("power_play_goals",   z))
        .withColumn("short_handed_goals", F.coalesce("short_handed_goals", z))
        .withColumn("penalty_minutes",    F.coalesce("penalty_minutes",    z))
        .withColumn("standings_points",   F.col("wins") * 2 + F.col("ot_losses"))
        .withColumn("goal_differential",  F.col("goals_for") - F.col("goals_against"))
        .withColumn(
            "win_pct",
            F.when(
                F.col("games_played") > 0,
                F.round(F.col("wins").cast("double") / F.col("games_played"), 4),
            ),
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "season", "game_type", "team_id", "team_abbrev", "team_name",
            "games_played", "wins", "losses", "ot_losses",
            "standings_points", "win_pct",
            "goals_for", "goals_against", "goal_differential",
            "shots_for", "shots_against",
            "power_play_goals", "short_handed_goals", "penalty_minutes",
            "ingestion_timestamp",
        )
    )


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.gold")
logger.info("Gold schema ready.")

logger.info(f"Reading silver table: {SILVER_TABLE}")
silver = spark.table(SILVER_TABLE).cache()
row_count = silver.count()
logger.info(f"Silver table loaded — {row_count:,} play rows.")

logger.info("Building player stats…")
player_stats = build_player_stats(silver)
logger.info(f"Writing {PLAYER_STATS_TABLE}…")
(
    player_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PLAYER_STATS_TABLE)
)
ps_count = spark.table(PLAYER_STATS_TABLE).count()
logger.info(f"Player stats written — {ps_count:,} player-season rows.")

logger.info("Building team stats…")
team_stats = build_team_stats(silver)
logger.info(f"Writing {TEAM_STATS_TABLE}…")
(
    team_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TEAM_STATS_TABLE)
)
ts_count = spark.table(TEAM_STATS_TABLE).count()
logger.info(f"Team stats written — {ts_count:,} team-season rows.")

silver.unpersist()
logger.info("batch_gold complete.")
