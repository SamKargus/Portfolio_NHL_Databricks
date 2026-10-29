"""
Live poller — fetches play-by-play for NHL games in progress and drops every
changed snapshot into the landing Volume, where the live pipeline
(src/live/live_pipeline.py) picks it up with Auto Loader.

Run it as a serverless Databricks job on a cron, e.g. every 30 minutes from
11:00 to 23:30 America/New_York, with max concurrent runs = 1 (overlapping
triggers are skipped while a run is active). Each run:

  * exits straight away if no game is in progress or starting within LOOKAHEAD
    — so idle triggers cost a few seconds of compute;
  * otherwise starts the live pipeline, polls every game in progress every
    POLL_INTERVAL_S (in parallel), writes a snapshot only when the game
    changed, follows each game until it's final, then lets the pipeline drain
    and stops it.

A game is followed from its own play-by-play response once it's picked up, so
late games keep being polled after the schedule rolls over to the next day.

Job parameters (notebook widgets, or --name=value task arguments):
  pipeline_id  id of the live Lakeflow pipeline; empty = don't start/stop it
  teams        comma-separated team abbreviations to follow, e.g. "TOR,MTL";
               empty = every game. Fewer games = less compute on Free Edition.
"""

import hashlib
import json
import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"live_poller_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("live_poller")
logger.info(f"live_poller started — log: {log_file}")

# Notebooks get `spark` injected; a job's Python script task may not.
try:
    spark  # noqa: F821
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

BASE_URL        = "https://api-web.nhle.com/v1"
LANDING_VOLUME  = "nhl.bronze.pbp_live_landing"
LANDING_PATH    = "/Volumes/nhl/bronze/pbp_live_landing"
PROCESSED_TABLE = "nhl.silver.processed_games"   # games the nightly batch has finished

POLL_INTERVAL_S      = 10    # per-game play-by-play poll while games are on
DISCOVERY_INTERVAL_S = 60    # how often to re-read today's schedule
LOOKAHEAD            = timedelta(minutes=35)   # wait for games starting this soon (> cron interval)
LATE_START_GRACE     = timedelta(minutes=30)   # a FUT game this far past start is still "upcoming"
DRAIN_WAIT_S         = 120   # let the pipeline ingest the last files before stopping it
MAX_RUNTIME          = timedelta(hours=14)
MAX_WORKERS          = 8

ACTIVE_STATES = {"PRE", "LIVE", "CRIT"}
FINAL_STATES  = {"OFF", "FINAL"}


def _param(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)  # noqa: F821 — notebook task
    except Exception:
        pass
    for arg in sys.argv[1:]:              # python file task: --name=value
        if arg.startswith(f"--{name}="):
            return arg.split("=", 1)[1]
    return default


PIPELINE_ID = _param("pipeline_id").strip()
TEAMS = {t.strip().upper() for t in _param("teams").split(",") if t.strip()}


# ----------------------------------------------------------------------
# NHL API
# ----------------------------------------------------------------------
def _get_json(url: str, attempts: int = 3):
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning(f"429 for {url}, waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            logger.warning(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e} (attempt {attempt})")
            time.sleep(2 ** attempt)
    return None


def discover_games() -> dict:
    """
    {game_id: game} for today's and yesterday's schedule (yesterday catches
    late games if the schedule has already rolled over), filtered to TEAMS.
    """
    games = {}
    today = _get_json(f"{BASE_URL}/score/now")   # redirects to /score/<today>
    if not today:
        return games
    days = [today]
    if today.get("prevDate"):
        prev = _get_json(f"{BASE_URL}/score/{today['prevDate']}")
        if prev:
            days.append(prev)
    for day in days:
        for g in day.get("games", []):
            teams = {(g.get("homeTeam") or {}).get("abbrev"), (g.get("awayTeam") or {}).get("abbrev")}
            if TEAMS and not (teams & TEAMS):
                continue
            games[g["id"]] = g
    return games


def fetch_play_by_play(game_id: int):
    """(game_id, fetched_at, data or None)"""
    data = _get_json(f"{BASE_URL}/gamecenter/{game_id}/play-by-play")
    return game_id, datetime.now(timezone.utc), data


def fingerprint(data: dict) -> str:
    """Changes when plays, scores or state change — not on every clock tick."""
    away, home = data.get("awayTeam") or {}, data.get("homeTeam") or {}
    key = [
        data.get("gameState"),
        data.get("plays"),
        away.get("score"), away.get("sog"),
        home.get("score"), home.get("sog"),
    ]
    return hashlib.sha1(json.dumps(key, sort_keys=True).encode()).hexdigest()


def write_snapshot(game_id: int, fetched_at: datetime, data: dict) -> None:
    # The raw API response, untouched. Volume files only become visible once
    # fully written (object storage), so Auto Loader never reads half a file.
    path = f"{LANDING_PATH}/{game_id}/{fetched_at:%Y%m%dT%H%M%SZ}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


# ----------------------------------------------------------------------
# Live pipeline control
# ----------------------------------------------------------------------
def start_pipeline() -> None:
    if not PIPELINE_ID:
        logger.info("No pipeline_id set — not starting the live pipeline")
        return
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    state = getattr(w.pipelines.get(PIPELINE_ID).state, "value", None)
    if state not in (None, "IDLE", "FAILED"):
        logger.info(f"Live pipeline already {state}")
        return
    w.pipelines.start_update(pipeline_id=PIPELINE_ID)
    logger.info(f"Started live pipeline {PIPELINE_ID}")


def stop_pipeline() -> None:
    if not PIPELINE_ID:
        return
    from databricks.sdk import WorkspaceClient
    WorkspaceClient().pipelines.stop(PIPELINE_ID)
    logger.info(f"Stopping live pipeline {PIPELINE_ID}")


# ----------------------------------------------------------------------
# Landing housekeeping
# ----------------------------------------------------------------------
def clean_landing() -> None:
    """Delete landing folders for games the nightly batch has finished."""
    try:
        folders = [d for d in os.listdir(LANDING_PATH) if d.isdigit()]
        if not folders:
            return
        done = {
            str(r.game_id)
            for r in spark.sql(  # noqa: F821
                f"SELECT DISTINCT game_id FROM {PROCESSED_TABLE} WHERE game_id IN ({','.join(folders)})"
            ).collect()
        }
        for d in done:
            shutil.rmtree(os.path.join(LANDING_PATH, d))
        if done:
            logger.info(f"Removed landing files for {len(done)} game(s) now in batch silver")
    except Exception as e:
        logger.warning(f"Landing cleanup skipped: {e}")


# ----------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {LANDING_VOLUME}")  # noqa: F821
    clean_landing()
    logger.info(f"Following {'teams ' + ','.join(sorted(TEAMS)) if TEAMS else 'all games'}")

    run_start = datetime.now(timezone.utc)
    tracked: dict = {}        # game_id -> fingerprint of the last snapshot written
    finished: set = set()
    upcoming: list = []
    last_discovery = 0.0
    pipeline_started = False
    snapshots_written = 0

    try:
        while True:
            now = datetime.now(timezone.utc)
            if now - run_start > MAX_RUNTIME:
                logger.warning(f"Max runtime {MAX_RUNTIME} reached — exiting")
                break

            if time.time() - last_discovery >= DISCOVERY_INTERVAL_S:
                games = discover_games()
                last_discovery = time.time()
                for gid, g in games.items():
                    if g.get("gameState") in ACTIVE_STATES and gid not in tracked and gid not in finished:
                        tracked[gid] = None
                        logger.info(f"Following game {gid} ({g.get('awayTeam', {}).get('abbrev')} @ "
                                    f"{g.get('homeTeam', {}).get('abbrev')}, {g.get('gameState')})")
                upcoming = [
                    gid for gid, g in games.items()
                    if g.get("gameState") == "FUT"
                    and g.get("gameScheduleState") == "OK"
                    and g.get("startTimeUTC")
                    and -LATE_START_GRACE
                        <= datetime.fromisoformat(g["startTimeUTC"].replace("Z", "+00:00")) - now
                        <= LOOKAHEAD
                ]

            if not tracked:
                if upcoming:
                    time.sleep(DISCOVERY_INTERVAL_S)   # puck drop soon — wait for it
                    continue
                logger.info("No games in progress or starting soon")
                break

            if not pipeline_started:
                start_pipeline()
                pipeline_started = True

            cycle_start = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                results = list(pool.map(fetch_play_by_play, list(tracked)))

            for gid, fetched_at, data in results:
                if data is None:
                    continue  # one bad request never holds up the other games
                fp = fingerprint(data)
                if fp != tracked[gid]:
                    write_snapshot(gid, fetched_at, data)
                    tracked[gid] = fp
                    snapshots_written += 1
                if data.get("gameState") in FINAL_STATES:
                    logger.info(f"Game {gid} is {data['gameState']} — final snapshot written, done with it")
                    finished.add(gid)
                    del tracked[gid]

            time.sleep(max(0.0, POLL_INTERVAL_S - (time.time() - cycle_start)))

        if pipeline_started:
            logger.info(f"Waiting {DRAIN_WAIT_S}s for the pipeline to ingest the last snapshots")
            time.sleep(DRAIN_WAIT_S)
    finally:
        if pipeline_started:
            stop_pipeline()  # unprocessed files are picked up on the next start

    logger.info(f"Done. Games finished={len(finished)} | Snapshots written={snapshots_written}")
