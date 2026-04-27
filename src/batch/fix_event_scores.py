"""
fix_event_scores.py
Corrects home_score / away_score on non-goal events in silver_game_events.

The bug: every event carries the final score of the game instead of the
score at the moment the event occurred.

Fix: for each non-goal event, the correct score is the score from the most
recent goal whose game_second <= the event's game_second.  If no goal has
happened yet the score is 0-0.

Goal events already have the correct score and are left untouched.
"""

import logging
import sys
from bisect import bisect_right
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parents[2]))
from src.utils.db import PG_PARAMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fix_event_scores")

UPDATE_PAGE  = 500    # rows per execute_batch call
COMMIT_EVERY = 200    # games between commits


def correct_score_at(goal_seconds: list, goal_scores: list, game_second: int):
    """Return (home, away) at the moment game_second occurs."""
    idx = bisect_right(goal_seconds, game_second) - 1
    return goal_scores[idx] if idx >= 0 else (0, 0)


def fix_game(cur, game_id: int) -> int:
    """
    Fix non-goal event scores for one game.
    Returns the number of rows that were updated.
    """
    cur.execute(
        """
        SELECT event_id, event_type, game_second, home_score, away_score
        FROM silver_game_events
        WHERE game_id = %s AND game_second IS NOT NULL
        ORDER BY game_second
        """,
        (game_id,),
    )
    rows = cur.fetchall()

    # Build the score timeline from goals
    goal_seconds: list[int] = []
    goal_scores:  list[tuple] = []
    for _, etype, gs, hs, as_ in rows:
        if etype == "goal":
            goal_seconds.append(gs)
            goal_scores.append((hs, as_))

    updates = []
    for event_id, etype, game_second, cur_home, cur_away in rows:
        if etype == "goal":
            continue
        correct_home, correct_away = correct_score_at(goal_seconds, goal_scores, game_second)
        if correct_home != cur_home or correct_away != cur_away:
            updates.append((correct_home, correct_away, event_id, game_id))

    if updates:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE silver_game_events
               SET home_score = %s, away_score = %s
             WHERE event_id = %s AND game_id = %s
            """,
            updates,
            page_size=UPDATE_PAGE,
        )

    return len(updates)


def main():
    conn = psycopg2.connect(**PG_PARAMS)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT game_id FROM silver_game_events ORDER BY game_id")
    game_ids = [r[0] for r in cur.fetchall()]
    total_games = len(game_ids)
    log.info("Starting — %d games to process", total_games)

    total_updated = 0
    for i, game_id in enumerate(game_ids, 1):
        updated = fix_game(cur, game_id)
        total_updated += updated

        if i % COMMIT_EVERY == 0:
            conn.commit()
            pct = i / total_games * 100
            log.info(
                "Progress: %d / %d games (%.1f%%) — %d rows corrected so far",
                i, total_games, pct, total_updated,
            )

    conn.commit()
    cur.close()
    conn.close()
    log.info("Done — %d rows corrected across %d games", total_updated, total_games)


if __name__ == "__main__":
    main()
