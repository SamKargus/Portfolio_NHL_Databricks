"""
Gold layer — aggregated player and team statistics derived from the silver tables.

Produces three tables:
  nhl.gold.player_stats  — per-player, per-season, per-game-type aggregates
                           (goals, assists, points, shots, hits, faceoffs, PIM, …)
  nhl.gold.team_stats    — per-team, per-season, per-game-type aggregates
                           (wins, losses, goals, shots, PP goals, SH goals, PIM, …)
  nhl.gold.dim_players   — one row per player (most recent known name, position,
                           team, sweater number, headshot URL) sourced from
                           nhl.silver.nhl_players; join to player_stats on player_id

All tables are fully overwritten on each run so they always reflect current
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
DIM_PLAYERS_TABLE  = "nhl.gold.dim_players"

# situation_code layout: [away_goalie][away_skaters][home_skaters][home_goalie]
# e.g. "1551" = even strength 5v5 with both goalies on ice
_SC_AWAY_SK = F.col("situation_code").substr(2, 1).cast("int")
_SC_HOME_SK = F.col("situation_code").substr(3, 1).cast("int")


# ----------------------------------------------------------------------
# Player stats
# ----------------------------------------------------------------------
def build_player_stats(silver):
    """
    Returns a DataFrame with one row per (season, game_type, player_name).

    Columns:
      season, game_type, player_name,
      games_played, goals, primary_assists, secondary_assists, assists, points,
      shots_on_goal, shooting_pct,
      hits, blocked_shots,
      faceoff_wins, faceoff_losses, faceoff_pct,
      penalty_minutes, giveaways, takeaways,
      ingestion_timestamp
    """
    GC = ["season", "game_type"]

    # ---- games_played ------------------------------------------------
    player_name_cols = [
        "scoring_player_name",
        "assist1_player_name", "assist2_player_name", "assist3_player_name",
        "shooting_player_name", "goalie_in_net_name",
        "hitting_player_name", "hittee_player_name",
        "blocking_player_name",
        "winning_player_name", "losing_player_name",
        "committed_by_player_name", "drawn_by_player_name", "served_by_player_name",
        "player_name",
    ]
    appearances = None
    for col_name in player_name_cols:
        subset = (
            silver.filter(F.col(col_name).isNotNull())
            .select(*GC, "game_id", F.col(col_name).alias("pname"))
        )
        appearances = subset if appearances is None else appearances.union(subset)

    games_played = (
        appearances.distinct()
        .groupBy(*GC, "pname")
        .agg(F.countDistinct("game_id").alias("games_played"))
        .withColumnRenamed("pname", "player_name")
    )

    # ---- goals -------------------------------------------------------
    goals = (
        silver.filter(F.col("type_desc_key") == "goal")
        .filter(F.col("scoring_player_name").isNotNull())
        .groupBy(*GC, F.col("scoring_player_name").alias("player_name"))
        .agg(F.count("*").alias("goals"))
    )

    # ---- primary assists ---------------------------------------------
    primary_assists = (
        silver.filter(F.col("type_desc_key") == "goal")
        .filter(F.col("assist1_player_name").isNotNull())
        .groupBy(*GC, F.col("assist1_player_name").alias("player_name"))
        .agg(F.count("*").alias("primary_assists"))
    )

    # ---- secondary assists (assist2 + assist3 combined) --------------
    secondary_assists = (
        silver.filter(F.col("type_desc_key") == "goal")
        .select(*GC, "assist2_player_name", "assist3_player_name")
        .select(
            *GC,
            F.explode(
                F.array(F.col("assist2_player_name"), F.col("assist3_player_name"))
            ).alias("player_name"),
        )
        .filter(F.col("player_name").isNotNull())
        .groupBy(*GC, "player_name")
        .agg(F.count("*").alias("secondary_assists"))
    )

    # ---- shots on goal (goals count as shots) -----------------------
    shots_on_goal = (
        silver.filter(F.col("type_desc_key").isin("shot-on-goal", "goal"))
        .filter(F.col("shooting_player_name").isNotNull())
        .groupBy(*GC, F.col("shooting_player_name").alias("player_name"))
        .agg(F.count("*").alias("shots_on_goal"))
    )

    # ---- hits (the hitter, not the recipient) -----------------------
    hits = (
        silver.filter(F.col("type_desc_key") == "hit")
        .filter(F.col("hitting_player_name").isNotNull())
        .groupBy(*GC, F.col("hitting_player_name").alias("player_name"))
        .agg(F.count("*").alias("hits"))
    )

    # ---- blocked shots (the defender who blocked) ------------------
    blocked_shots = (
        silver.filter(F.col("type_desc_key") == "blocked-shot")
        .filter(F.col("blocking_player_name").isNotNull())
        .groupBy(*GC, F.col("blocking_player_name").alias("player_name"))
        .agg(F.count("*").alias("blocked_shots"))
    )

    # ---- faceoffs ---------------------------------------------------
    faceoff_wins = (
        silver.filter(F.col("type_desc_key") == "faceoff")
        .filter(F.col("winning_player_name").isNotNull())
        .groupBy(*GC, F.col("winning_player_name").alias("player_name"))
        .agg(F.count("*").alias("faceoff_wins"))
    )

    faceoff_losses = (
        silver.filter(F.col("type_desc_key") == "faceoff")
        .filter(F.col("losing_player_name").isNotNull())
        .groupBy(*GC, F.col("losing_player_name").alias("player_name"))
        .agg(F.count("*").alias("faceoff_losses"))
    )

    # ---- penalty minutes -------------------------------------------
    pim = (
        silver.filter(F.col("type_desc_key") == "penalty")
        .filter(F.col("committed_by_player_name").isNotNull())
        .filter(F.col("penalty_duration").isNotNull())
        .groupBy(*GC, F.col("committed_by_player_name").alias("player_name"))
        .agg(F.sum("penalty_duration").alias("penalty_minutes"))
    )

    # ---- giveaways & takeaways -------------------------------------
    giveaways = (
        silver.filter(F.col("type_desc_key") == "giveaway")
        .filter(F.col("player_name").isNotNull())
        .groupBy(*GC, "player_name")
        .agg(F.count("*").alias("giveaways"))
    )

    takeaways = (
        silver.filter(F.col("type_desc_key") == "takeaway")
        .filter(F.col("player_name").isNotNull())
        .groupBy(*GC, "player_name")
        .agg(F.count("*").alias("takeaways"))
    )

    # ---- join & derive computed columns ----------------------------
    z = F.lit(0).cast("bigint")

    return (
        games_played
        .join(goals,             [*GC, "player_name"], "left")
        .join(primary_assists,   [*GC, "player_name"], "left")
        .join(secondary_assists, [*GC, "player_name"], "left")
        .join(shots_on_goal,     [*GC, "player_name"], "left")
        .join(hits,              [*GC, "player_name"], "left")
        .join(blocked_shots,     [*GC, "player_name"], "left")
        .join(faceoff_wins,      [*GC, "player_name"], "left")
        .join(faceoff_losses,    [*GC, "player_name"], "left")
        .join(pim,               [*GC, "player_name"], "left")
        .join(giveaways,         [*GC, "player_name"], "left")
        .join(takeaways,         [*GC, "player_name"], "left")
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
            "season", "game_type", "player_name",
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
# Player dimension
# ----------------------------------------------------------------------
def build_dim_players(silver):
    """
    Returns a DataFrame with one row per player_name.

    player_id is a stable integer derived from hashing the player_name —
    consistent across runs and used as the surrogate key for joins.

    Columns: player_id, player_name, ingestion_timestamp
    """
    player_name_cols = [
        "scoring_player_name",
        "assist1_player_name", "assist2_player_name", "assist3_player_name",
        "shooting_player_name", "goalie_in_net_name",
        "hitting_player_name", "hittee_player_name",
        "blocking_player_name",
        "winning_player_name", "losing_player_name",
        "committed_by_player_name", "drawn_by_player_name", "served_by_player_name",
        "player_name",
    ]

    all_names = None
    for col_name in player_name_cols:
        subset = silver.filter(F.col(col_name).isNotNull()).select(F.col(col_name).alias("player_name"))
        all_names = subset if all_names is None else all_names.union(subset)

    return (
        all_names
        .dropDuplicates(["player_name"])
        .withColumn("player_id", F.abs(F.hash("player_name")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select("player_id", "player_name", "ingestion_timestamp")
    )


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS nhl.gold")
logger.info("Gold schema ready.")

silver = spark.table(SILVER_TABLE)

logger.info("Building player stats…")
(
    build_player_stats(silver).write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PLAYER_STATS_TABLE)
)
logger.info(f"Written: {PLAYER_STATS_TABLE}")

logger.info("Building team stats…")
(
    build_team_stats(silver).write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TEAM_STATS_TABLE)
)
logger.info(f"Written: {TEAM_STATS_TABLE}")

logger.info("Building player dimension…")
(
    build_dim_players(silver).write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_PLAYERS_TABLE)
)
logger.info(f"Written: {DIM_PLAYERS_TABLE}")

logger.info("batch_gold complete.")
