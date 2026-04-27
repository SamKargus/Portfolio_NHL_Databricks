"""
historical_backfill.py
Fetches all historical NHL game data (2010-11 → present) and writes it
directly to PostgreSQL, applying the same Bronze/Silver/Gold transformations
as the streaming pipeline.

Usage:
    python3 src/batch/historical_backfill.py                      # all seasons 2010→now
    python3 src/batch/historical_backfill.py --start-season 2022  # from 2022-23 onward
    python3 src/batch/historical_backfill.py --season 2024        # just 2024-25 season

Progress is tracked in the backfill_log table — safe to kill and resume.
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).parents[2]))

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "historical_backfill.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("backfill")

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "nhl_pipeline"),
    user=os.getenv("PG_USER", os.getenv("USER", "samkargus")),
    password=os.getenv("PG_PASSWORD", ""),
)

NHL_BASE     = "https://api-web.nhle.com/v1"
RATE_DELAY   = 0.6   # seconds between API calls (~100 req/min, well under limits)
GAME_TYPES   = {2: "regular", 3: "playoff"}


# ── HTTP session ───────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "NHLBackfill/1.0"
    return s


# ── Schedule helpers ───────────────────────────────────────────────────────────
def get_season_date_range(season_start_year: int) -> tuple[date, date]:
    """Return (season_start, season_end) dates for a given season start year."""
    start = date(season_start_year, 9, 15)   # pre-season starts mid-Sept
    end   = date(season_start_year + 1, 7, 1)  # playoffs end by July
    return start, end


def fetch_game_ids_for_week(session: requests.Session, week_date: date) -> list[dict]:
    """Return list of {game_id, season, game_type, game_date, state} for a schedule week."""
    url  = f"{NHL_BASE}/schedule/{week_date.isoformat()}"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("Schedule fetch failed for %s: %s", week_date, exc)
        return []

    games = []
    for day in data.get("gameWeek", []):
        game_date = day.get("date", "")
        for g in day.get("games", []):
            state = g.get("gameState", "")
            games.append({
                "game_id":   g["id"],
                "season":    g.get("season"),
                "game_type": g.get("gameType"),
                "game_date": game_date,
                "state":     state,
            })
    return games


def iter_season_weeks(season_start_year: int):
    """Yield successive week start dates covering one full season."""
    start, end = get_season_date_range(season_start_year)
    current = start
    while current < end:
        yield current
        current += timedelta(weeks=1)


# ── Data extraction (mirrors nhl_api_producer logic) ──────────────────────────

def parse_mm_ss(time_str: str | None) -> int | None:
    if not time_str:
        return None
    try:
        parts = time_str.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return None


def extract_events(pbp: dict, game_id: int) -> list[dict]:
    home = pbp.get("homeTeam", {})
    away = pbp.get("awayTeam", {})
    now  = datetime.now(timezone.utc)

    rows = []
    for play in pbp.get("plays", []):
        type_code = play.get("typeDescKey", "").lower()
        details   = play.get("details", {})
        pd_       = play.get("periodDescriptor", {})

        period    = pd_.get("number")
        tip_str   = play.get("timeInPeriod")
        trm_str   = play.get("timeRemaining")
        tip_secs  = parse_mm_ss(tip_str)
        trm_secs  = parse_mm_ss(trm_str)
        game_sec  = ((period - 1) * 1200 + tip_secs) if period and tip_secs is not None else None
        h_score   = details.get("homeScore") if details.get("homeScore") is not None else home.get("score")
        a_score   = details.get("awayScore") if details.get("awayScore") is not None else away.get("score")

        rows.append({
            "event_id":               f"{game_id}_{play.get('eventId', 0)}",
            "game_id":                game_id,
            "season":                 pbp.get("season"),
            "game_type":              pbp.get("gameType"),
            "event_type":             type_code,
            "period":                 period,
            "period_type":            pd_.get("periodType"),
            "time_in_period":         tip_str,
            "time_remaining":         trm_str,
            "time_in_period_secs":    tip_secs,
            "time_remaining_secs":    trm_secs,
            "game_second":            game_sec,
            "home_team_id":           home.get("id"),
            "home_team_abbrev":       home.get("abbrev"),
            "away_team_id":           away.get("id"),
            "away_team_abbrev":       away.get("abbrev"),
            "home_score":             h_score,
            "away_score":             a_score,
            "score_differential":     (h_score - a_score) if h_score is not None and a_score is not None else None,
            "zone_code":              details.get("zoneCode"),
            "x_coord":                details.get("xCoord"),
            "y_coord":                details.get("yCoord"),
            "scoring_player_id":      details.get("scoringPlayerId"),
            "scoring_player_total":   details.get("scoringPlayerTotal"),
            "assist1_player_id":      details.get("assist1PlayerId"),
            "assist1_player_total":   details.get("assist1PlayerTotal"),
            "assist2_player_id":      details.get("assist2PlayerId"),
            "assist2_player_total":   details.get("assist2PlayerTotal"),
            "goalie_in_net_id":       details.get("goalieInNetId"),
            "shot_type":              details.get("shotType"),
            "goal_type":              details.get("goalModifier"),
            "penalty_type":           details.get("typeCode"),
            "duration_minutes":       details.get("duration"),
            "committed_by_player_id": details.get("committedByPlayerId"),
            "drawn_by_player_id":     details.get("drawnByPlayerId"),
            "hittee_player_id":       details.get("hitteePlayerId"),
            "hitting_player_id":      details.get("hittingPlayerId"),
            "player_id":              details.get("playerId"),
            "is_overtime":            (period > 3) if period else False,
            "is_penalty_shot":        details.get("goalModifier") == "penalty-shot",
            "kafka_timestamp":        now,
            "bronze_ingested_at":     now,
            "silver_processed_at":    now,
        })
    return rows


def extract_player_stats(boxscore: dict, game_id: int) -> list[dict]:
    pbgs = boxscore.get("playerByGameStats", {})
    now  = datetime.now(timezone.utc)
    rows = []

    for side in ("homeTeam", "awayTeam"):
        team_meta   = boxscore.get(side, {})
        team_id     = team_meta.get("id")
        team_abbrev = team_meta.get("abbrev")
        team_stats  = pbgs.get(side, {})

        for player in team_stats.get("forwards", []) + team_stats.get("defense", []):
            rows.append({
                "stat_id":        f"{game_id}_{team_id}_{player.get('playerId')}",
                "game_id":        game_id,
                "season":         boxscore.get("season"),
                "team_id":        team_id,
                "team_abbrev":    team_abbrev,
                "player_id":      player.get("playerId"),
                "player_name":    player.get("name", {}).get("default"),
                "position":       player.get("position"),
                "sweater_number": player.get("sweaterNumber"),
                "goals":          player.get("goals", 0),
                "assists":        player.get("assists", 0),
                "points":         player.get("points", 0),
                "plus_minus":     player.get("plusMinus", 0),
                "pim":            player.get("pim", 0),
                "hits":           player.get("hits", 0),
                "blocked_shots":  player.get("blockedShots", 0),
                "shots":          player.get("sog", 0),
                "giveaways":      player.get("giveaways", 0),
                "takeaways":      player.get("takeaways", 0),
                "toi_seconds":    parse_mm_ss(player.get("toi")),
                "kafka_timestamp":     now,
                "silver_processed_at": now,
            })

        for goalie in team_stats.get("goalies", []):
            ssa = goalie.get("saveShotsAgainst", "0/0").split("/")
            rows.append({
                "stat_id":        f"{game_id}_{team_id}_{goalie.get('playerId')}",
                "game_id":        game_id,
                "season":         boxscore.get("season"),
                "team_id":        team_id,
                "team_abbrev":    team_abbrev,
                "player_id":      goalie.get("playerId"),
                "player_name":    goalie.get("name", {}).get("default"),
                "position":       "G",
                "sweater_number": goalie.get("sweaterNumber"),
                "saves":          ssa[0] if ssa else None,
                "shots_against":  ssa[-1] if ssa else None,
                "save_pct":       goalie.get("savePctg"),
                "goals_against":  goalie.get("goalsAgainst", 0),
                "toi_seconds":    parse_mm_ss(goalie.get("toi")),
                "pim":            goalie.get("pim", 0),
                "kafka_timestamp":     now,
                "silver_processed_at": now,
            })

    return rows


def extract_players(pbp: dict) -> list[dict]:
    rows = []
    for s in pbp.get("rosterSpots", []):
        first = s.get("firstName", {}).get("default", "")
        last  = s.get("lastName",  {}).get("default", "")
        rows.append({
            "player_id":  s.get("playerId"),
            "full_name":  f"{first} {last}".strip(),
            "first_name": first,
            "last_name":  last,
            "position":   s.get("positionCode"),
        })
    return rows


def build_gold(events: list[dict], game_id: int) -> dict:
    """Compute gold aggregates from a list of event dicts."""
    from collections import defaultdict

    scoring: dict = defaultdict(lambda: {"goals": 0, "primary_assists": 0, "secondary_assists": 0, "last_event_at": None})
    penalties: dict = defaultdict(lambda: {"penalty_count": 0, "total_penalty_minutes": 0, "offenders": set(), "last_penalty_at": None})
    momentum: list = []

    for e in events:
        etype = e.get("event_type", "")
        ts    = e.get("kafka_timestamp")

        if etype == "goal":
            if e.get("scoring_player_id"):
                scoring[(game_id, e["scoring_player_id"])]["goals"] += 1
                scoring[(game_id, e["scoring_player_id"])]["last_event_at"] = ts
            if e.get("assist1_player_id"):
                scoring[(game_id, e["assist1_player_id"])]["primary_assists"] += 1
                scoring[(game_id, e["assist1_player_id"])]["last_event_at"] = ts
            if e.get("assist2_player_id"):
                scoring[(game_id, e["assist2_player_id"])]["secondary_assists"] += 1
                scoring[(game_id, e["assist2_player_id"])]["last_event_at"] = ts

        elif etype == "penalty":
            ptype = e.get("penalty_type") or "unknown"
            penalties[(game_id, ptype)]["penalty_count"] += 1
            penalties[(game_id, ptype)]["total_penalty_minutes"] += (e.get("duration_minutes") or 0)
            if e.get("committed_by_player_id"):
                penalties[(game_id, ptype)]["offenders"].add(e["committed_by_player_id"])
            penalties[(game_id, ptype)]["last_penalty_at"] = ts

    now = datetime.now(timezone.utc)

    gold_scoring = []
    for (gid, pid), s in scoring.items():
        ta = s["primary_assists"] + s["secondary_assists"]
        gold_scoring.append({
            "game_id":           gid,
            "player_id":         pid,
            "goals":             s["goals"],
            "primary_assists":   s["primary_assists"],
            "secondary_assists": s["secondary_assists"],
            "total_assists":     ta,
            "points":            s["goals"] + ta,
            "last_event_at":     s["last_event_at"],
            "gold_updated_at":   now,
        })

    gold_penalties = []
    for (gid, ptype), p in penalties.items():
        gold_penalties.append({
            "game_id":               gid,
            "penalty_type":          ptype,
            "penalty_count":         p["penalty_count"],
            "total_penalty_minutes": p["total_penalty_minutes"],
            "unique_offenders":      len(p["offenders"]),
            "last_penalty_at":       p["last_penalty_at"],
            "gold_updated_at":       now,
        })

    return {"scoring": gold_scoring, "penalties": gold_penalties}


# ── PostgreSQL writers ─────────────────────────────────────────────────────────

BRONZE_EVENT_COLS = [
    "event_id", "game_id", "season", "game_type", "event_type", "period",
    "period_type", "time_in_period", "time_remaining", "home_team_id",
    "home_team_abbrev", "away_team_id", "away_team_abbrev", "home_score",
    "away_score", "scoring_player_id", "assist1_player_id", "assist2_player_id",
    "goalie_in_net_id", "shot_type", "goal_type", "committed_by_player_id",
    "drawn_by_player_id", "penalty_type", "duration_minutes", "x_coord",
    "y_coord", "zone_code", "hittee_player_id", "hitting_player_id",
    "player_id", "kafka_timestamp", "bronze_ingested_at",
]

SILVER_EVENT_COLS = [
    "event_id", "game_id", "season", "game_type", "event_type", "period",
    "period_type", "time_in_period", "time_remaining", "time_in_period_secs",
    "time_remaining_secs", "game_second", "home_team_id", "home_team_abbrev",
    "away_team_id", "away_team_abbrev", "home_score", "away_score",
    "score_differential", "zone_code", "x_coord", "y_coord",
    "scoring_player_id", "scoring_player_total", "assist1_player_id",
    "assist1_player_total", "assist2_player_id", "assist2_player_total",
    "goalie_in_net_id", "shot_type", "goal_type", "penalty_type",
    "duration_minutes", "committed_by_player_id", "drawn_by_player_id",
    "hittee_player_id", "hitting_player_id", "player_id",
    "is_overtime", "is_penalty_shot", "kafka_timestamp", "silver_processed_at",
]

SILVER_STAT_COLS = [
    "stat_id", "game_id", "season", "team_id", "team_abbrev", "player_id",
    "player_name", "position", "sweater_number", "goals", "assists", "points",
    "plus_minus", "pim", "hits", "blocked_shots", "shots",
    "giveaways", "takeaways", "toi_seconds",
    "kafka_timestamp", "silver_processed_at",
]


def _pick(row: dict, cols: list[str]) -> dict:
    return {c: row.get(c) for c in cols}


def write_game(conn, events: list[dict], stats: list[dict], gold: dict, player_rows: list[dict]) -> None:
    with conn.cursor() as cur:
        # Bronze events
        if events:
            psycopg2.extras.execute_batch(cur, _insert_ignore_sql("bronze_game_events", BRONZE_EVENT_COLS),
                                          [_pick(e, BRONZE_EVENT_COLS) for e in events], page_size=500)

        # Silver events
        if events:
            psycopg2.extras.execute_batch(cur, _insert_ignore_sql("silver_game_events", SILVER_EVENT_COLS),
                                          [_pick(e, SILVER_EVENT_COLS) for e in events], page_size=500)

        # Silver stats
        if stats:
            valid_cols = [c for c in SILVER_STAT_COLS if c in (stats[0].keys() if stats else [])]
            if valid_cols:
                psycopg2.extras.execute_batch(cur, _insert_ignore_sql("silver_player_stats", valid_cols),
                                              [_pick(s, valid_cols) for s in stats], page_size=500)

        # Gold scoring
        if gold["scoring"]:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO gold_player_scoring
                    (game_id, player_id, goals, primary_assists, secondary_assists,
                     total_assists, points, last_event_at, gold_updated_at)
                VALUES (%(game_id)s, %(player_id)s, %(goals)s, %(primary_assists)s,
                        %(secondary_assists)s, %(total_assists)s, %(points)s,
                        %(last_event_at)s, %(gold_updated_at)s)
                ON CONFLICT (game_id, player_id) DO UPDATE SET
                    goals             = EXCLUDED.goals,
                    primary_assists   = EXCLUDED.primary_assists,
                    secondary_assists = EXCLUDED.secondary_assists,
                    total_assists     = EXCLUDED.total_assists,
                    points            = EXCLUDED.points,
                    last_event_at     = EXCLUDED.last_event_at,
                    gold_updated_at   = EXCLUDED.gold_updated_at
            """, gold["scoring"], page_size=200)

        # Gold penalties
        if gold["penalties"]:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO gold_penalty_summary
                    (game_id, penalty_type, penalty_count, total_penalty_minutes,
                     unique_offenders, last_penalty_at, gold_updated_at)
                VALUES (%(game_id)s, %(penalty_type)s, %(penalty_count)s,
                        %(total_penalty_minutes)s, %(unique_offenders)s,
                        %(last_penalty_at)s, %(gold_updated_at)s)
                ON CONFLICT (game_id, penalty_type) DO UPDATE SET
                    penalty_count         = EXCLUDED.penalty_count,
                    total_penalty_minutes = EXCLUDED.total_penalty_minutes,
                    unique_offenders      = EXCLUDED.unique_offenders,
                    last_penalty_at       = EXCLUDED.last_penalty_at,
                    gold_updated_at       = EXCLUDED.gold_updated_at
            """, gold["penalties"], page_size=200)

        # Players lookup
        if player_rows:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO players (player_id, full_name, first_name, last_name, position, updated_at)
                VALUES (%(player_id)s, %(full_name)s, %(first_name)s, %(last_name)s, %(position)s, NOW())
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name  = EXCLUDED.full_name,
                    first_name = EXCLUDED.first_name,
                    last_name  = EXCLUDED.last_name,
                    position   = EXCLUDED.position
            """, player_rows, page_size=200)

    conn.commit()


def _insert_ignore_sql(table: str, cols: list[str]) -> str:
    col_str = ", ".join(cols)
    val_str = ", ".join(f"%({c})s" for c in cols)
    return f"INSERT INTO {table} ({col_str}) VALUES ({val_str}) ON CONFLICT DO NOTHING"


def mark_done(conn, game_id: int, season: int, game_type: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO backfill_log (game_id, season, game_type, status, processed_at)
            VALUES (%s, %s, %s, 'done', NOW())
            ON CONFLICT (game_id) DO UPDATE SET status='done', processed_at=NOW()
        """, (game_id, season, game_type))
    conn.commit()


def mark_error(conn, game_id: int, season: int, game_type: int, msg: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO backfill_log (game_id, season, game_type, status, processed_at, error_msg)
            VALUES (%s, %s, %s, 'error', NOW(), %s)
            ON CONFLICT (game_id) DO UPDATE SET status='error', processed_at=NOW(), error_msg=%s
        """, (game_id, season, game_type, msg, msg))
    conn.commit()


def get_done_ids(conn) -> set[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT game_id FROM backfill_log WHERE status = 'done'")
        return {row[0] for row in cur.fetchall()}


# ── Main backfill loop ─────────────────────────────────────────────────────────

def process_game(session: requests.Session, conn, game_meta: dict, done_ids: set[int]) -> bool:
    game_id   = game_meta["game_id"]
    game_type = game_meta.get("game_type", 0)

    if game_id in done_ids:
        return False   # already processed

    if game_meta.get("state") not in ("OFF", "FINAL", "OFFICIAL"):
        return False   # skip live / future games

    if game_type not in GAME_TYPES:
        return False   # skip preseason (game_type=1)

    try:
        time.sleep(RATE_DELAY)
        pbp = session.get(f"{NHL_BASE}/gamecenter/{game_id}/play-by-play", timeout=15).json()
        if not pbp.get("plays"):
            mark_done(conn, game_id, pbp.get("season", 0), game_type)
            return True

        time.sleep(RATE_DELAY)
        box = session.get(f"{NHL_BASE}/gamecenter/{game_id}/boxscore", timeout=15).json()

        events      = extract_events(pbp, game_id)
        stats       = extract_player_stats(box, game_id)
        player_rows = extract_players(pbp)
        gold        = build_gold(events, game_id)

        write_game(conn, events, stats, gold, player_rows)
        mark_done(conn, game_id, pbp.get("season", 0), game_type)
        done_ids.add(game_id)
        return True

    except Exception as exc:
        log.error("Game %d failed: %s", game_id, exc)
        mark_error(conn, game_id, game_meta.get("season", 0), game_type, str(exc))
        return False


def run_backfill(start_season: int, end_season: int) -> None:
    session = make_session()
    conn    = psycopg2.connect(**PG)
    done_ids = get_done_ids(conn)

    log.info("Starting backfill: seasons %d–%d  (%d already done)",
             start_season, end_season, len(done_ids))

    total_processed = 0

    for season_year in range(start_season, end_season + 1):
        log.info("── Season %d-%d ──", season_year, season_year + 1)
        season_processed = 0

        for week_start in iter_season_weeks(season_year):
            games = fetch_game_ids_for_week(session, week_start)
            time.sleep(RATE_DELAY)

            for game_meta in games:
                did_process = process_game(session, conn, game_meta, done_ids)
                if did_process:
                    season_processed += 1
                    total_processed  += 1
                    if total_processed % 50 == 0:
                        log.info("Progress: %d games processed so far", total_processed)

        log.info("Season %d-%d done: %d games processed", season_year, season_year + 1, season_processed)

    conn.close()
    log.info("Backfill complete — %d games total", total_processed)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NHL historical data backfill")
    parser.add_argument("--start-season", type=int, default=2010,
                        help="First season start year (default: 2010 for 2010-11)")
    parser.add_argument("--end-season",   type=int, default=2024,
                        help="Last season start year (default: 2024 for 2024-25)")
    parser.add_argument("--season",       type=int, default=None,
                        help="Backfill a single season (overrides start/end)")
    args = parser.parse_args()

    if args.season:
        run_backfill(args.season, args.season)
    else:
        run_backfill(args.start_season, args.end_season)


if __name__ == "__main__":
    main()
