# Databricks Notebook — NHL Playoff Dashboard Queries
# Attach to an interactive cluster with Delta tables mounted.
# Each cell is separated by # COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC ## NHL Playoff Analytics Dashboard

# COMMAND ----------
# ── Configuration ──────────────────────────────────────────────────────────────
GOLD_BASE = "dbfs:/mnt/nhl/gold"
HIST_BASE = "dbfs:/mnt/nhl/historical"

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------
# ── 1. Top scorers — current playoffs ─────────────────────────────────────────
top_scorers = spark.sql(f"""
    SELECT
        player_name,
        team_abbrev,
        position,
        SUM(goals)         AS goals,
        SUM(total_assists) AS assists,
        SUM(points)        AS points,
        COUNT(DISTINCT game_id) AS gp,
        ROUND(SUM(points) / COUNT(DISTINCT game_id), 2) AS ppg
    FROM delta.`{HIST_BASE}/player_season_stats`
    GROUP BY player_name, team_abbrev, position
    ORDER BY points DESC, goals DESC
    LIMIT 25
""")
display(top_scorers)

# COMMAND ----------
# ── 2. Real-time player scoring (live game gold table) ─────────────────────────
live_scoring = spark.sql(f"""
    SELECT
        game_id,
        player_id,
        goals,
        primary_assists,
        secondary_assists,
        total_assists,
        points,
        gold_updated_at
    FROM delta.`{GOLD_BASE}/player_scoring`
    ORDER BY points DESC, goals DESC
    LIMIT 50
""")
display(live_scoring)

# COMMAND ----------
# ── 3. Team momentum (last 30 minutes of game time) ───────────────────────────
team_momentum = spark.sql(f"""
    SELECT
        game_id,
        team_abbrev,
        window_start,
        window_end,
        momentum_score,
        SUM(momentum_score) OVER (
            PARTITION BY game_id, team_abbrev
            ORDER BY window_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_momentum
    FROM delta.`{GOLD_BASE}/team_momentum`
    ORDER BY game_id, team_abbrev, window_start DESC
""")
display(team_momentum)

# COMMAND ----------
# ── 4. Penalty summary per team ────────────────────────────────────────────────
penalty_summary = spark.sql(f"""
    SELECT
        game_id,
        penalty_type,
        penalty_count,
        total_penalty_minutes,
        unique_offenders
    FROM delta.`{GOLD_BASE}/penalty_summary`
    ORDER BY total_penalty_minutes DESC
""")
display(penalty_summary)

# COMMAND ----------
# ── 5. Plus-minus leaderboard ─────────────────────────────────────────────────
plus_minus = spark.sql(f"""
    SELECT
        player_name,
        team_abbrev,
        position,
        SUM(plus_minus)    AS plus_minus,
        SUM(goals)         AS goals,
        SUM(assists)       AS assists,
        SUM(toi_seconds) / 60.0 AS total_toi_minutes
    FROM delta.`{HIST_BASE}/player_season_stats`
    GROUP BY player_name, team_abbrev, position
    ORDER BY plus_minus DESC
    LIMIT 25
""")
display(plus_minus)

# COMMAND ----------
# ── 6. Playoff series standings ────────────────────────────────────────────────
series_standings = spark.sql(f"""
    SELECT
        team_a,
        team_b,
        team_a_wins,
        team_b_wins,
        games_played,
        CASE
            WHEN team_a_wins = 4 THEN CONCAT(team_a, ' wins series')
            WHEN team_b_wins = 4 THEN CONCAT(team_b, ' wins series')
            ELSE CONCAT(team_a, ' leads ', team_a_wins, '-', team_b_wins)
        END AS series_status
    FROM delta.`{HIST_BASE}/playoff_series`
    ORDER BY (team_a_wins + team_b_wins) DESC
""")
display(series_standings)

# COMMAND ----------
# ── 7. Team shooting efficiency ────────────────────────────────────────────────
shooting = spark.sql(f"""
    SELECT
        team_abbrev,
        SUM(goals)  AS goals,
        SUM(shots)  AS shots,
        ROUND(SUM(goals) / NULLIF(SUM(shots), 0) * 100, 2) AS shooting_pct,
        SUM(games_played) AS gp
    FROM delta.`{HIST_BASE}/player_season_stats`
    WHERE position != 'G'
    GROUP BY team_abbrev
    ORDER BY shooting_pct DESC
""")
display(shooting)

# COMMAND ----------
# ── 8. Game summaries with OT flag ────────────────────────────────────────────
game_summaries = spark.sql(f"""
    SELECT
        game_id,
        game_date,
        home_team_abbrev,
        away_team_abbrev,
        final_home_score,
        final_away_score,
        winner,
        periods_played,
        went_to_overtime,
        total_goals,
        total_shots_on_goal,
        total_penalties
    FROM delta.`{HIST_BASE}/game_summaries`
    ORDER BY game_date DESC, game_id
""")
display(game_summaries)

# COMMAND ----------
# ── 9. Goal scoring by period ─────────────────────────────────────────────────
goals_by_period = spark.sql(f"""
    SELECT
        period,
        period_type,
        COUNT(*) AS goals,
        COUNT(DISTINCT game_id) AS games,
        ROUND(COUNT(*) / COUNT(DISTINCT game_id), 2) AS goals_per_game
    FROM delta.`{GOLD_BASE}/player_scoring`
    -- join back to events to get period; or query silver directly:
    -- FROM delta.`dbfs:/mnt/nhl/silver/game_events`
    -- WHERE event_type = 'goal'
    GROUP BY period, period_type
    ORDER BY period
""")
display(goals_by_period)

# COMMAND ----------
# ── 10. Shot location heatmap data ────────────────────────────────────────────
shot_locations = spark.sql("""
    SELECT
        x_coord,
        y_coord,
        zone_code,
        event_type,
        shot_type,
        COUNT(*) AS event_count
    FROM delta.`dbfs:/mnt/nhl/silver/game_events`
    WHERE event_type IN ('goal', 'shot-on-goal', 'missed-shot', 'blocked-shot')
      AND x_coord IS NOT NULL
      AND y_coord IS NOT NULL
    GROUP BY x_coord, y_coord, zone_code, event_type, shot_type
    ORDER BY event_count DESC
""")
display(shot_locations)

# COMMAND ----------
# ── 11. Bronze data freshness check ───────────────────────────────────────────
freshness = spark.sql("""
    SELECT
        'bronze_game_events' AS table_name,
        MAX(bronze_ingested_at) AS latest_record,
        COUNT(*) AS total_records,
        TIMESTAMPDIFF(MINUTE, MAX(bronze_ingested_at), CURRENT_TIMESTAMP()) AS minutes_since_last_record
    FROM delta.`dbfs:/mnt/nhl/bronze/game_events`
    UNION ALL
    SELECT
        'bronze_player_stats',
        MAX(bronze_ingested_at),
        COUNT(*),
        TIMESTAMPDIFF(MINUTE, MAX(bronze_ingested_at), CURRENT_TIMESTAMP())
    FROM delta.`dbfs:/mnt/nhl/bronze/player_stats`
""")
display(freshness)
