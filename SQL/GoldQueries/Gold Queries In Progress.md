  Two things worth knowing up front, both a consequence of your design:
  - The NHL API only returns rich play details for roughly 2010‑present, even though bronze goes back to 1917. So these play-grain queries are effectively "modern era." Add a dim_game/dim_date
  season filter if you want a specific window. 
  - "Shots on goal" in the fact are the saved shots (type_desc_key='shot-on-goal'); goals are a separate event. So a player's shot attempts = goals + shots-on-goal, which several queries below rely
  on.

  ---
  Players
  
  1. All-time goal scorers
  SELECT p.full_name, t.abbrev AS current_team, COUNT(*) AS goals
  FROM nhl.gold.fact_event f
  JOIN nhl.gold.dim_player p ON f.scoring_player_id = p.player_id
  LEFT JOIN nhl.gold.dim_team t ON p.team_id = t.team_id
  WHERE f.type_desc_key = 'goal'
  GROUP BY p.full_name, t.abbrev
  ORDER BY goals DESC
  LIMIT 25;
  
  2. Top playmakers (assists, all three assist slots unpivoted)
  WITH assists AS (
    SELECT assist1_player_id AS pid FROM nhl.gold.fact_event WHERE type_desc_key='goal'
    UNION ALL SELECT assist2_player_id FROM nhl.gold.fact_event WHERE type_desc_key='goal'
    UNION ALL SELECT assist3_player_id FROM nhl.gold.fact_event WHERE type_desc_key='goal'
  ) 
  SELECT p.full_name, COUNT(*) AS assists
  FROM assists a
  JOIN nhl.gold.dim_player p ON a.pid = p.player_id
  WHERE a.pid IS NOT NULL
  GROUP BY p.full_name
  ORDER BY assists DESC
  LIMIT 25;
  
  3. Best shooting % (min 200 attempts) — the snipers
  WITH attempts AS (
    SELECT scoring_player_id AS pid, 1 AS is_goal FROM nhl.gold.fact_event WHERE type_desc_key='goal'
    UNION ALL
    SELECT shooting_player_id AS pid, 0 AS is_goal FROM nhl.gold.fact_event WHERE type_desc_key='shot-on-goal'
  ) 
  SELECT p.full_name, p.position_code,
         SUM(is_goal) AS goals,
         COUNT(*)     AS shots_on_goal,
         ROUND(100.0 * SUM(is_goal) / COUNT(*), 1) AS shooting_pct
  FROM attempts a
  JOIN nhl.gold.dim_player p ON a.pid = p.player_id
  WHERE a.pid IS NOT NULL
  GROUP BY p.full_name, p.position_code
  HAVING COUNT(*) >= 200
  ORDER BY shooting_pct DESC
  LIMIT 25;

  4. Which shot type does each player score with best?
  SELECT shot_type,
         COUNT(*) AS attempts,
         SUM(CASE WHEN type_desc_key='goal' THEN 1 ELSE 0 END) AS goals,
         ROUND(100.0 * SUM(CASE WHEN type_desc_key='goal' THEN 1 ELSE 0 END) / COUNT(*), 1) AS conversion_pct
  FROM nhl.gold.fact_event
  WHERE type_desc_key IN ('goal','shot-on-goal') AND shot_type IS NOT NULL
  GROUP BY shot_type
  ORDER BY conversion_pct DESC;

  5. The heavy hitters — and who absorbs the most
  SELECT p.full_name,
         SUM(CASE WHEN f.hitting_player_id = p.player_id THEN 1 ELSE 0 END) AS hits_thrown,
         SUM(CASE WHEN f.hittee_player_id  = p.player_id THEN 1 ELSE 0 END) AS hits_taken
  FROM nhl.gold.fact_event f
  JOIN nhl.gold.dim_player p
    ON p.player_id IN (f.hitting_player_id, f.hittee_player_id)
  WHERE f.type_desc_key = 'hit'
  GROUP BY p.full_name
  ORDER BY hits_thrown DESC
  LIMIT 25;

  6. Faceoff win % leaders (min 500 draws)
  WITH fo AS (
    SELECT winning_player_id AS pid, 1 AS won FROM nhl.gold.fact_event WHERE type_desc_key='faceoff'
    UNION ALL
    SELECT losing_player_id  AS pid, 0 AS won FROM nhl.gold.fact_event WHERE type_desc_key='faceoff'
  ) 
  SELECT p.full_name,
         COUNT(*) AS draws,
         ROUND(100.0 * SUM(won) / COUNT(*), 1) AS faceoff_win_pct
  FROM fo
  JOIN nhl.gold.dim_player p ON fo.pid = p.player_id
  WHERE fo.pid IS NOT NULL 
  GROUP BY p.full_name
  HAVING COUNT(*) >= 500
  ORDER BY faceoff_win_pct DESC
  LIMIT 25;

  7. Penalty minutes (the box residents) vs. penalties drawn (the agitators)
  SELECT p.full_name,
         SUM(CASE WHEN f.committed_by_player_id = p.player_id THEN f.penalty_duration ELSE 0 END) AS pim,
         SUM(CASE WHEN f.drawn_by_player_id     = p.player_id THEN 1 ELSE 0 END)                  AS penalties_drawn
  FROM nhl.gold.fact_event f
  JOIN nhl.gold.dim_player p
    ON p.player_id IN (f.committed_by_player_id, f.drawn_by_player_id)
  WHERE f.type_desc_key = 'penalty'
  GROUP BY p.full_name
  ORDER BY pim DESC
  LIMIT 25;

  8. Shot-blocking machines

  SELECT p.full_name, p.position_code, COUNT(*) AS shots_blocked
  FROM nhl.gold.fact_event f
  JOIN nhl.gold.dim_player p ON f.blocking_player_id = p.player_id
  WHERE f.type_desc_key = 'blocked-shot'
  GROUP BY p.full_name, p.position_code
  ORDER BY shots_blocked DESC
  LIMIT 25;

  9. Most-traveled players (uses your dim_player_team stint table)
  SELECT p.full_name,
         COUNT(*)                          AS stints,
         COUNT(DISTINCT pt.team_id)        AS distinct_teams,
         MIN(pt.valid_from)                AS first_seen,
         MAX(pt.valid_to)                  AS last_seen
  FROM nhl.gold.dim_player_team pt         
  JOIN nhl.gold.dim_player p ON pt.player_id = p.player_id
  GROUP BY p.full_name
  HAVING COUNT(DISTINCT pt.team_id) > 1
  ORDER BY distinct_teams DESC, stints DESC
  LIMIT 25;

  Teams & games

  10. Goalies under fire — shots faced & implied save % (min 1000 shots)
  SELECT p.full_name,
         COUNT(*) AS shots_faced,
         SUM(CASE WHEN f.type_desc_key='goal' THEN 1 ELSE 0 END) AS goals_allowed,
         ROUND(100.0 * (1 - SUM(CASE WHEN f.type_desc_key='goal' THEN 1 ELSE 0 END)/COUNT(*)), 2) AS save_pct
  FROM nhl.gold.fact_event f
  JOIN nhl.gold.dim_player p ON f.goalie_in_net_id = p.player_id
  WHERE f.type_desc_key IN ('goal','shot-on-goal') 
  GROUP BY p.full_name
  HAVING COUNT(*) >= 1000
  ORDER BY save_pct DESC
  LIMIT 25;

  11. Team home-ice advantage — win % home vs away, by season
  WITH results AS (
    SELECT season, home_team_id AS team_id, 'home' AS site,
           CASE WHEN home_final_score > away_final_score THEN 1 ELSE 0 END AS win
    FROM nhl.gold.dim_game WHERE game_type = 2
    UNION ALL
    SELECT season, away_team_id, 'away',
           CASE WHEN away_final_score > home_final_score THEN 1 ELSE 0 END
    FROM nhl.gold.dim_game WHERE game_type = 2
  ) 
  SELECT t.abbrev,
         ROUND(100.0*AVG(CASE WHEN site='home' THEN win END),1) AS home_win_pct,
         ROUND(100.0*AVG(CASE WHEN site='away' THEN win END),1) AS away_win_pct
  FROM results r
  JOIN nhl.gold.dim_team t ON r.team_id = t.team_id
  GROUP BY t.abbrev
  ORDER BY (home_win_pct - away_win_pct) DESC;

  12. Highest-scoring games ever recorded
  SELECT g.game_date,
         ht.abbrev AS home, g.home_final_score,
         at.abbrev AS away, g.away_final_score,
         g.home_final_score + g.away_final_score AS total_goals,
         g.last_period_type 
  FROM nhl.gold.dim_game g
  JOIN nhl.gold.dim_team ht ON g.home_team_id = ht.team_id
  JOIN nhl.gold.dim_team at ON g.away_team_id = at.team_id
  ORDER BY total_goals DESC 
  LIMIT 20;

  13. When do goals actually happen? (period / OT distribution)
  SELECT period_number, period_type, COUNT(*) AS goals
  FROM nhl.gold.fact_event
  WHERE type_desc_key = 'goal'
  GROUP BY period_number, period_type
  ORDER BY goals DESC;
  14. Are weekend games higher-scoring? (uses dim_date)
  SELECT d.is_weekend, d.day_name,
         COUNT(DISTINCT g.game_id) AS games,
         ROUND(AVG(g.home_final_score + g.away_final_score), 2) AS avg_total_goals
  FROM nhl.gold.dim_game g
  JOIN nhl.gold.dim_date d ON g.date_key = d.date_key
  GROUP BY d.is_weekend, d.day_name
  ORDER BY avg_total_goals DESC;
  
  ---
  A couple of notes so these behave:
  - To scope any of these to a season, add JOIN nhl.gold.dim_game g ON f.game_id = g.game_id WHERE g.season = 20232024 (regular season is game_type = 2, playoffs = 3).
  - Query 10's "save %" is implied from play events, so it can differ slightly from official save % (empty-net situations, etc.) — fine for exploration, not for a record book.