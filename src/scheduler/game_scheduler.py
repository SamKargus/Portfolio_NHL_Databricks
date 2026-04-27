"""
game_scheduler.py
Polls the NHL API schedule every N minutes.  When a game is about to start
it launches the bronze/silver/gold streaming scripts as local subprocesses.
When all games for the day are over it terminates them cleanly.

The nightly batch job (nightly_batch_job.py) is also launched locally via
subprocess at 04:00 UTC.

No Databricks API required — runs entirely on your local machine.
"""

import logging
import os
import re
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parents[2]
LOG_DIR  = ROOT / "logs"
SRC_DIR  = ROOT / "src"
LOG_DIR.mkdir(exist_ok=True)

STREAMING_SCRIPTS = [
    SRC_DIR / "streaming" / "bronze_streaming.py",
    SRC_DIR / "streaming" / "silver_streaming.py",
    SRC_DIR / "streaming" / "gold_streaming.py",
]
PRODUCER_SCRIPT = SRC_DIR / "ingestion" / "nhl_api_producer.py"
BATCH_SCRIPT    = SRC_DIR / "batch" / "nightly_batch_job.py"

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "game_scheduler.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("game_scheduler")


# ── Config ─────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = ROOT / "config" / "config.yaml"
    with open(config_path) as f:
        raw = f.read()
    def _expand(m):
        val = os.getenv(m.group(1))
        if val is not None:
            return val
        return m.group(2) if m.group(2) is not None else m.group(0)
    raw = re.sub(r"\$\{(\w+)(?::-([^}]*))?\}", _expand, raw)
    return yaml.safe_load(raw)


# ── HTTP session ───────────────────────────────────────────────────────────────
def _make_session(retries: int = 3) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "NHLScheduler/1.0"
    return session


# ── NHL API helpers ────────────────────────────────────────────────────────────
NHL_BASE = "https://api-web.nhle.com/v1"


def fetch_todays_games(session: requests.Session) -> list[dict]:
    url = f"{NHL_BASE}/schedule/now"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("Failed to fetch schedule: %s", exc)
        return []

    games = []
    today = datetime.now(timezone.utc).date().isoformat()
    for game_week in data.get("gameWeek", []):
        if game_week.get("date", "") == today:
            games.extend(game_week.get("games", []))
    return games


def get_game_state(game: dict) -> str:
    return game.get("gameState", "UNKNOWN")


def parse_start_time(game: dict) -> Optional[datetime]:
    raw = game.get("startTimeUTC")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


# ── Kafka manager ─────────────────────────────────────────────────────────────
class KafkaManager:
    """Starts and stops the local Kafka broker via brew services."""

    def is_running(self) -> bool:
        result = subprocess.run(
            ["pgrep", "-f", "kafka.Kafka"],
            capture_output=True,
        )
        return result.returncode == 0

    def start(self) -> None:
        if self.is_running():
            log.info("Kafka already running — skipping start.")
            return
        log.info("Starting Kafka …")
        result = subprocess.run(
            ["brew", "services", "start", "kafka"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            log.info("Kafka started.")
        else:
            log.error("Failed to start Kafka: %s", result.stderr.strip())

    def stop(self) -> None:
        if not self.is_running():
            return
        log.info("Stopping Kafka …")
        result = subprocess.run(
            ["brew", "services", "stop", "kafka"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            log.info("Kafka stopped.")
        else:
            log.error("Failed to stop Kafka: %s", result.stderr.strip())


# ── Local process manager ──────────────────────────────────────────────────────
class LocalStreamingManager:
    """
    Starts and stops the three streaming scripts as local subprocesses.
    Each script writes its own log to logs/<script_name>.log.
    """

    def __init__(self):
        self._processes: dict[str, subprocess.Popen] = {}

    def start(self) -> None:
        if self.is_running():
            log.info("Streaming processes already running — skipping start.")
            return
        self._processes.clear()   # drop any stale Popen handles from prior run

        env = {**os.environ}   # inherit current shell env (includes sourced .env vars)

        for script in [PRODUCER_SCRIPT] + STREAMING_SCRIPTS:
            name = script.stem
            log_path = LOG_DIR / f"{name}.log"
            log_file = open(log_path, "a")

            log.info("Starting %s (log → %s) …", name, log_path)
            proc = subprocess.Popen(
                [sys.executable, str(script)],
                stdout=log_file,
                stderr=log_file,
                env=env,
                start_new_session=True,   # detach from scheduler's process group
            )
            self._processes[name] = proc
            log.info("  PID %d — %s", proc.pid, name)

    def stop(self) -> None:
        if not self._processes:
            return

        log.info("Stopping %d streaming process(es) …", len(self._processes))
        for name, proc in self._processes.items():
            if proc.poll() is None:          # still running
                log.info("  Terminating PID %d (%s) …", proc.pid, name)
                try:
                    proc.terminate()
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    log.warning("  PID %d did not exit — killing.", proc.pid)
                    proc.kill()
            else:
                log.info("  %s already exited (code %d).", name, proc.returncode)

        self._processes.clear()
        log.info("All streaming processes stopped.")

    def is_running(self) -> bool:
        """True if at least one streaming process is still alive."""
        return any(p.poll() is None for p in self._processes.values())

    def health_check(self) -> None:
        """Log a warning for any process that died unexpectedly."""
        for name, proc in list(self._processes.items()):
            if proc.poll() is not None:
                log.warning(
                    "Process '%s' (PID %d) exited unexpectedly with code %d.",
                    name, proc.pid, proc.returncode,
                )


# ── Core scheduler logic ───────────────────────────────────────────────────────
class GameScheduler:
    def __init__(self, cfg: dict):
        self.cfg       = cfg
        self.sched_cfg = cfg["scheduler"]
        self.nhl_session   = _make_session()
        self.kafka_mgr     = KafkaManager()
        self.stream_mgr    = LocalStreamingManager()
        self._start_buffer = timedelta(minutes=self.sched_cfg["game_start_buffer_minutes"])
        self._active_game_ids: set[int] = set()

    def check_games(self) -> None:
        now   = datetime.now(timezone.utc)
        games = fetch_todays_games(self.nhl_session)

        if not games:
            log.debug("No games scheduled today.")
            return

        upcoming, live, finished = [], [], []
        for game in games:
            state = get_game_state(game)
            if state in ("LIVE", "CRIT"):
                live.append(game)
            elif state in ("FUT", "PRE"):
                start_time = parse_start_time(game)
                if start_time and (start_time - now) <= self._start_buffer:
                    upcoming.append(game)
            elif state in ("OFF", "FINAL", "OFFICIAL"):
                finished.append(game)

        log.info(
            "Schedule check — upcoming=%d  live=%d  finished=%d",
            len(upcoming), len(live), len(finished),
        )

        should_stream = bool(upcoming or live)
        all_done = bool(games) and all(
            get_game_state(g) in ("OFF", "FINAL", "OFFICIAL") for g in games
        )

        if should_stream and not self.stream_mgr.is_running():
            game_ids = [g["id"] for g in live + upcoming]
            self._active_game_ids = set(game_ids)
            log.info("Games starting — launching pipeline for: %s", game_ids)
            self.kafka_mgr.start()
            self.stream_mgr.start()

        elif all_done and self.stream_mgr.is_running():
            log.info("All games finished — stopping pipeline.")
            self.stream_mgr.stop()
            self.kafka_mgr.stop()
            self._active_game_ids.clear()

        elif self.stream_mgr.is_running():
            # Periodic health check — log any crashed processes
            self.stream_mgr.health_check()

    def trigger_nightly_batch(self) -> None:
        """Run nightly_batch_job.py as a blocking subprocess at 04:00 UTC."""
        log.info("Starting nightly batch job …")
        log_path = LOG_DIR / "nightly_batch.log"
        try:
            with open(log_path, "a") as log_file:
                result = subprocess.run(
                    [sys.executable, str(BATCH_SCRIPT)],
                    stdout=log_file,
                    stderr=log_file,
                    env={**os.environ},
                    timeout=7200,   # 2-hour max
                )
            if result.returncode == 0:
                log.info("Nightly batch completed successfully.")
            else:
                log.error("Nightly batch exited with code %d — check %s", result.returncode, log_path)
        except subprocess.TimeoutExpired:
            log.error("Nightly batch timed out after 2 hours.")
        except Exception as exc:
            log.error("Nightly batch failed: %s", exc)

    def shutdown(self) -> None:
        """Gracefully stop all pipeline processes on scheduler exit."""
        log.info("Scheduler shutting down …")
        self.stream_mgr.stop()
        self.kafka_mgr.stop()


# ── APScheduler setup ──────────────────────────────────────────────────────────
def main() -> None:
    cfg = load_config()
    monitor = GameScheduler(cfg)

    check_interval = cfg["scheduler"]["check_interval_minutes"]
    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        monitor.check_games,
        trigger=IntervalTrigger(minutes=check_interval),
        id="game_monitor",
        name="NHL Game Monitor",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    scheduler.add_job(
        monitor.trigger_nightly_batch,
        trigger="cron",
        hour=4,
        minute=0,
        id="nightly_batch",
        name="NHL Nightly Batch",
        replace_existing=True,
        max_instances=1,
    )

    log.info(
        "Scheduler started — game check every %d min, batch at 04:00 UTC.",
        check_interval,
    )

    try:
        monitor.check_games()   # run immediately on startup
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        monitor.shutdown()
        scheduler.shutdown(wait=False)
        log.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
