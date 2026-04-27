"""
backfill_game_dates.py
One-shot utility to populate the game_date column on existing rows.

Walks the NHL weekly schedule API to build a {game_id: game_date} map,
then UPDATEs silver_game_events, bronze_game_events, and game_scores.
Idempotent — only touches rows where game_date IS NULL.

Usage:
    python3 src/batch/backfill_game_dates.py                         # all seasons present in DB
    python3 src/batch/backfill_game_dates.py --season 2024           # just 2024-25
    python3 src/batch/backfill_game_dates.py --start-season 2020 --end-season 2025
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).parents[2]))

LOG_DIR = Path(__file__).parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "backfill_game_dates.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("backfill_dates")

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "nhl_pipeline"),
    user=os.getenv("PG_USER", os.getenv("USER", "samkargus")),
    password=os.getenv("PG_PASSWORD", ""),
)

NHL_BASE   = "https://api-web.nhle.com/v1"
RATE_DELAY = 0.5


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "NHLDateBackfill/1.0"
    return s


def season_range(season_start_year: int) -> tuple[date, date]:
    """Pre-season begins mid-Sept; playoffs end by early July the next year."""
    return date(season_start_year, 9, 15), date(season_start_year + 1, 7, 1)


def iter_weeks(start: date, end: date):
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(weeks=1)


def fetch_week_dates(session: requests.Session, week_start: date) -> dict[int, date]:
    """Return {game_id: game_date} for the ISO week containing week_start."""
    url = f"{NHL_BASE}/schedule/{week_start.isoformat()}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("schedule fetch failed for %s: %s", week_start, exc)
        return {}

    out: dict[int, date] = {}
    for day in data.get("gameWeek", []):
        raw = day.get("date", "")
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            continue
        for g in day.get("games", []):
            gid = g.get("id")
            if gid:
                out[int(gid)] = d
    return out


def fetch_game_date_single(session: requests.Session, game_id: int) -> date | None:
    """Fallback for games the schedule API doesn't cover (very old seasons)."""
    url = f"{NHL_BASE}/gamecenter/{game_id}/play-by-play"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        raw = resp.json().get("gameDate")
        return date.fromisoformat(raw) if raw else None
    except Exception:
        return None


def get_seasons_in_db(conn) -> list[int]:
    """Seasons (as start year) with rows still missing game_date."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT season FROM silver_game_events
            WHERE game_date IS NULL AND season IS NOT NULL
            ORDER BY season
        """)
        return [int(r[0]) // 10000 for r in cur.fetchall()]


def get_null_game_ids(conn, season_start: int) -> set[int]:
    """Game IDs in this season still missing a date."""
    season_code = season_start * 10000 + (season_start + 1)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT game_id FROM silver_game_events
            WHERE season = %s AND game_date IS NULL
        """, (season_code,))
        return {int(r[0]) for r in cur.fetchall()}


def apply_dates(conn, mapping: dict[int, date]) -> int:
    """UPDATE the three tables with (game_id -> date). Returns rows touched in silver."""
    if not mapping:
        return 0
    rows = [{"gid": gid, "gd": d} for gid, d in mapping.items()]
    updated = 0
    with conn.cursor() as cur:
        for table in ("silver_game_events", "bronze_game_events", "game_scores"):
            sql = f"""
                UPDATE {table} SET game_date = %(gd)s
                WHERE game_id = %(gid)s AND game_date IS NULL
            """
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            if table == "silver_game_events":
                updated = cur.rowcount
    conn.commit()
    return updated


def backfill_season(session: requests.Session, conn, season_start: int, fallback: bool) -> None:
    need = get_null_game_ids(conn, season_start)
    if not need:
        log.info("Season %d-%d: nothing to do.", season_start, season_start + 1)
        return
    log.info("Season %d-%d: %d games need a date.", season_start, season_start + 1, len(need))

    start, end = season_range(season_start)
    found: dict[int, date] = {}

    for week_start in iter_weeks(start, end):
        if not need - set(found.keys()):
            break
        chunk = fetch_week_dates(session, week_start)
        time.sleep(RATE_DELAY)
        if chunk:
            for gid, d in chunk.items():
                if gid in need:
                    found[gid] = d

    apply_dates(conn, found)
    covered = set(found.keys()) & need
    log.info("Season %d-%d: schedule API covered %d/%d.",
             season_start, season_start + 1, len(covered), len(need))

    missing = need - covered
    if missing and fallback:
        log.info("Season %d-%d: falling back to per-game lookup for %d games.",
                 season_start, season_start + 1, len(missing))
        fallback_map: dict[int, date] = {}
        for i, gid in enumerate(sorted(missing), 1):
            d = fetch_game_date_single(session, gid)
            time.sleep(RATE_DELAY)
            if d:
                fallback_map[gid] = d
            if i % 50 == 0:
                log.info("  ... %d/%d", i, len(missing))
                apply_dates(conn, fallback_map)
                fallback_map.clear()
        apply_dates(conn, fallback_map)


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate game_date on existing NHL rows.")
    parser.add_argument("--season", type=int, help="Single season start year (e.g. 2024 for 2024-25)")
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season",   type=int)
    parser.add_argument("--no-fallback",  action="store_true",
                        help="Skip the slow per-game fallback for games not on the weekly schedule.")
    args = parser.parse_args()

    conn = psycopg2.connect(**PG)
    session = make_session()

    if args.season is not None:
        seasons = [args.season]
    elif args.start_season is not None and args.end_season is not None:
        seasons = list(range(args.start_season, args.end_season + 1))
    else:
        seasons = get_seasons_in_db(conn)
        log.info("Auto-detected %d seasons with NULL game_date.", len(seasons))

    for s in seasons:
        backfill_season(session, conn, s, fallback=not args.no_fallback)

    conn.close()
    log.info("Backfill complete.")


if __name__ == "__main__":
    main()
