"""
nhl_api_producer.py
Polls the NHL Stats API every N seconds during live games, extracts
play-by-play events, player stats, and game state, then writes them as
NDJSON landing files to DBFS for downstream Auto Loader ingestion.

Runs as a Python task in a Databricks Job (no Spark context needed).
Landing directories:
  dbfs:/mnt/nhl/landing/game_events/   — play-by-play events
  dbfs:/mnt/nhl/landing/player_stats/  — per-poll boxscore snapshots
  dbfs:/mnt/nhl/landing/game_state/    — live clock / score info
  dbfs:/mnt/nhl/landing/players/       — roster name lookups
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("nhl_producer")

# ── Config ─────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    import re
    _this = globals().get("__file__") or sys._getframe(0).f_code.co_filename
    config_path = Path(_this).parents[2] / "config" / "config.yaml"
    with open(config_path) as f:
        raw = f.read()
    def _expand(m):
        val = os.getenv(m.group(1))
        if val is not None:
            return val
        return m.group(2) if m.group(2) is not None else m.group(0)
    raw = re.sub(r"\$\{(\w+)(?::-([^}]*))?\}", _expand, raw)
    return yaml.safe_load(raw)


# ── Landing paths (Unity Catalog Volume, auto-detected when env var not set) ────
_uc_base      = f"/Volumes/{spark.catalog.currentCatalog()}/nhl/nhl_data"
_LANDING_BASE = os.getenv("LANDING_BASE") or f"{_uc_base}/landing"
LANDING_EVENTS      = f"{_LANDING_BASE}/game_events"
LANDING_STATS       = f"{_LANDING_BASE}/player_stats"
LANDING_GAME_STATE  = f"{_LANDING_BASE}/game_state"
LANDING_PLAYERS     = f"{_LANDING_BASE}/players"

# Lightweight producer checkpoint — persists seen events across restarts
CHECKPOINT_PATH = f"{_LANDING_BASE}/../producer_checkpoint.json"

def _ensure_dirs() -> None:
    for d in (LANDING_EVENTS, LANDING_STATS, LANDING_GAME_STATE, LANDING_PLAYERS):
        Path(d).mkdir(parents=True, exist_ok=True)


# ── Rate-limited HTTP session ──────────────────────────────────────────────────
class RateLimitedSession:
    def __init__(self, requests_per_minute: int, timeout: int, max_retries: int):
        self._min_interval = 60.0 / requests_per_minute
        self._last_call    = 0.0
        self._timeout      = timeout
        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self._session = requests.Session()
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.headers.update({
            "User-Agent": "NHLPlayoffAnalytics/1.0",
            "Accept":     "application/json",
        })

    def get(self, url: str, **kwargs) -> requests.Response:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        try:
            resp = self._session.get(url, timeout=self._timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                retry_after = int(exc.response.headers.get("Retry-After", 30))
                log.warning("Rate limited — sleeping %d s", retry_after)
                time.sleep(retry_after)
                return self.get(url, **kwargs)
            raise
        finally:
            self._last_call = time.monotonic()


# ── NHL API helpers ────────────────────────────────────────────────────────────
BASE_URL = "https://api-web.nhle.com/v1"

RELEVANT_EVENT_TYPES = {
    "goal", "shot-on-goal", "penalty", "hit",
    "faceoff", "blocked-shot", "missed-shot", "giveaway", "takeaway",
}


def fetch_live_game_ids(session: RateLimitedSession) -> list[int]:
    try:
        data = session.get(f"{BASE_URL}/schedule/now").json()
    except Exception as exc:
        log.error("Failed to fetch schedule: %s", exc)
        return []
    game_ids = []
    for game_week in data.get("gameWeek", []):
        for game in game_week.get("games", []):
            if game.get("gameState", "") in ("LIVE", "CRIT"):
                game_ids.append(game["id"])
    return game_ids


def fetch_play_by_play(session: RateLimitedSession, game_id: int) -> dict:
    try:
        return session.get(f"{BASE_URL}/gamecenter/{game_id}/play-by-play").json()
    except Exception as exc:
        log.error("play-by-play fetch failed for game %d: %s", game_id, exc)
        return {}


def fetch_boxscore(session: RateLimitedSession, game_id: int) -> dict:
    try:
        return session.get(f"{BASE_URL}/gamecenter/{game_id}/boxscore").json()
    except Exception as exc:
        log.error("boxscore fetch failed for game %d: %s", game_id, exc)
        return {}


# ── Extraction ─────────────────────────────────────────────────────────────────
def extract_game_events(pbp: dict, game_id: int) -> list[dict]:
    events      = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    home_team   = pbp.get("homeTeam", {})
    away_team   = pbp.get("awayTeam", {})

    for play in pbp.get("plays", []):
        type_code = play.get("typeDescKey", "").lower()
        if type_code not in RELEVANT_EVENT_TYPES:
            continue

        details     = play.get("details", {})
        period_desc = play.get("periodDescriptor", {})

        events.append({
            "event_id":               f"{game_id}_{play.get('eventId', 0)}",
            "game_id":                game_id,
            "season":                 pbp.get("season"),
            "game_type":              pbp.get("gameType"),
            "event_type":             type_code,
            "period":                 period_desc.get("number"),
            "period_type":            period_desc.get("periodType"),
            "time_in_period":         play.get("timeInPeriod"),
            "time_remaining":         play.get("timeRemaining"),
            "home_team_id":           home_team.get("id"),
            "home_team_abbrev":       home_team.get("abbrev"),
            "away_team_id":           away_team.get("id"),
            "away_team_abbrev":       away_team.get("abbrev"),
            "home_score":             details.get("homeScore") if details.get("homeScore") is not None else home_team.get("score"),
            "away_score":             details.get("awayScore") if details.get("awayScore") is not None else away_team.get("score"),
            "scoring_player_id":      details.get("scoringPlayerId"),
            "scoring_player_total":   details.get("scoringPlayerTotal"),
            "assist1_player_id":      details.get("assist1PlayerId"),
            "assist1_player_total":   details.get("assist1PlayerTotal"),
            "assist2_player_id":      details.get("assist2PlayerId"),
            "assist2_player_total":   details.get("assist2PlayerTotal"),
            "goalie_in_net_id":       details.get("goalieInNetId"),
            "shot_type":              details.get("shotType"),
            "goal_type":              details.get("goalModifier"),
            "committed_by_player_id": details.get("committedByPlayerId"),
            "drawn_by_player_id":     details.get("drawnByPlayerId"),
            "penalty_type":           details.get("typeCode"),
            "duration_minutes":       details.get("duration"),
            "x_coord":                details.get("xCoord"),
            "y_coord":                details.get("yCoord"),
            "zone_code":              details.get("zoneCode"),
            "hittee_player_id":       details.get("hitteePlayerId"),
            "hitting_player_id":      details.get("hittingPlayerId"),
            "player_id":              details.get("playerId"),
            "ingested_at":            ingested_at,
            "source":                 "nhl_api_v1",
        })
    return events


def extract_game_state(pbp: dict, game_id: int) -> dict:
    home        = pbp.get("homeTeam", {})
    away        = pbp.get("awayTeam", {})
    clock       = pbp.get("clock", {})
    period_desc = pbp.get("periodDescriptor", {})
    return {
        "game_id":          game_id,
        "home_team_abbrev": home.get("abbrev"),
        "away_team_abbrev": away.get("abbrev"),
        "home_score":       home.get("score", 0),
        "away_score":       away.get("score", 0),
        "period":           period_desc.get("number"),
        "period_type":      period_desc.get("periodType"),
        "time_remaining":   clock.get("timeRemaining"),
        "game_state":       pbp.get("gameState"),
        "in_intermission":  clock.get("inIntermission", False),
        "clock_running":    clock.get("running", False),
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }


def extract_player_stats(boxscore: dict, game_id: int) -> list[dict]:
    records     = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    pbgs        = boxscore.get("playerByGameStats", {})

    for side in ("homeTeam", "awayTeam"):
        team_meta   = boxscore.get(side, {})
        team_id     = team_meta.get("id")
        team_abbrev = team_meta.get("abbrev")
        team_stats  = pbgs.get(side, {})

        for player in team_stats.get("forwards", []) + team_stats.get("defense", []):
            records.append({
                "stat_id":       f"{game_id}_{team_id}_{player.get('playerId')}",
                "game_id":       game_id,
                "season":        boxscore.get("season"),
                "team_id":       team_id,
                "team_abbrev":   team_abbrev,
                "player_id":     player.get("playerId"),
                "player_name":   player.get("name", {}).get("default"),
                "position":      player.get("position"),
                "sweater_number":player.get("sweaterNumber"),
                "goals":         player.get("goals", 0),
                "assists":       player.get("assists", 0),
                "points":        player.get("points", 0),
                "plus_minus":    player.get("plusMinus", 0),
                "pim":           player.get("pim", 0),
                "hits":          player.get("hits", 0),
                "blocked_shots": player.get("blockedShots", 0),
                "shots":         player.get("sog", 0),
                "faceoff_wins":  player.get("faceoffWinningPctg", 0),
                "faceoff_attempts": 0,
                "giveaways":     player.get("giveaways", 0),
                "takeaways":     player.get("takeaways", 0),
                "time_on_ice":   player.get("toi"),
                "power_play_toi": player.get("powerPlayToi"),
                "short_handed_toi": player.get("shortHandedToi"),
                "ingested_at":   ingested_at,
                "source":        "nhl_api_v1",
            })

        for goalie in team_stats.get("goalies", []):
            sav = goalie.get("saveShotsAgainst", "0/0").split("/")
            records.append({
                "stat_id":        f"{game_id}_{team_id}_{goalie.get('playerId')}",
                "game_id":        game_id,
                "season":         boxscore.get("season"),
                "team_id":        team_id,
                "team_abbrev":    team_abbrev,
                "player_id":      goalie.get("playerId"),
                "player_name":    goalie.get("name", {}).get("default"),
                "position":       "G",
                "sweater_number": goalie.get("sweaterNumber"),
                "saves":          int(sav[0]) if sav[0].isdigit() else 0,
                "shots_against":  int(sav[-1]) if sav[-1].isdigit() else 0,
                "save_pct":       goalie.get("savePctg"),
                "goals_against":  goalie.get("goalsAgainst", 0),
                "time_on_ice":    goalie.get("toi"),
                "pim":            goalie.get("pim", 0),
                "ingested_at":    ingested_at,
                "source":         "nhl_api_v1",
            })
    return records


def extract_players(pbp: dict) -> list[dict]:
    players = []
    for s in pbp.get("rosterSpots", []):
        first = s.get("firstName", {}).get("default", "")
        last  = s.get("lastName",  {}).get("default", "")
        players.append({
            "player_id": s.get("playerId"),
            "full_name": f"{first} {last}".strip(),
            "first_name": first,
            "last_name":  last,
            "position":  s.get("positionCode"),
            "team_id":   s.get("teamId"),
        })
    return players


# ── Landing file writer ────────────────────────────────────────────────────────
def _write_ndjson(path: str, records: list[dict]) -> None:
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")


def _write_json(path: str, record: dict) -> None:
    with open(path, "w") as f:
        json.dump(record, f, default=str)


# ── Producer ───────────────────────────────────────────────────────────────────
class NHLProducer:
    def __init__(self, cfg: dict):
        self.cfg           = cfg
        self.api_cfg       = cfg["nhl_api"]
        self.poll_interval = self.api_cfg["poll_interval_seconds"]
        self.session       = RateLimitedSession(
            requests_per_minute=self.api_cfg["rate_limit_requests_per_minute"],
            timeout=self.api_cfg["timeout_seconds"],
            max_retries=self.api_cfg["max_retries"],
        )
        self._running         = True
        self._seen_events:    dict[int, dict[str, str]] = {}
        self._prev_live_ids:  set[int] = set()

        _ensure_dirs()
        self._load_checkpoint()

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_) -> None:
        log.info("Shutdown signal received.")
        self._running = False

    # ── Checkpoint: persist seen events so restarts don't re-emit everything ────
    def _load_checkpoint(self) -> None:
        try:
            with open(CHECKPOINT_PATH) as f:
                raw = json.load(f)
            # Keys are str (JSON), convert game_ids back to int
            self._seen_events = {int(k): v for k, v in raw.items()}
            log.info("Loaded checkpoint: %d game(s).", len(self._seen_events))
        except (FileNotFoundError, json.JSONDecodeError):
            self._seen_events = {}

    def _save_checkpoint(self) -> None:
        try:
            Path(CHECKPOINT_PATH).parent.mkdir(parents=True, exist_ok=True)
            with open(CHECKPOINT_PATH, "w") as f:
                json.dump(self._seen_events, f)
        except Exception as exc:
            log.warning("Failed to save checkpoint: %s", exc)

    # ── Core poll ──────────────────────────────────────────────────────────────
    def _poll_game(self, game_id: int) -> None:
        if game_id not in self._seen_events:
            self._seen_events[game_id] = {}

        pbp = fetch_play_by_play(self.session, game_id)
        if not pbp:
            return

        ts = int(time.time() * 1000)

        # Game state (always write — dashboard needs live clock)
        game_state = extract_game_state(pbp, game_id)
        _write_json(f"{LANDING_GAME_STATE}/game_{game_id}_{ts}.json", game_state)

        # Player roster (write once per game start)
        players = extract_players(pbp)
        if players:
            _write_ndjson(f"{LANDING_PLAYERS}/game_{game_id}_{ts}.ndjson", players)

        # Events — only new or reclassified
        all_events = extract_game_events(pbp, game_id)
        new_events = [
            e for e in all_events
            if self._seen_events[game_id].get(e["event_id"]) != e["event_type"]
        ]
        if new_events:
            _write_ndjson(f"{LANDING_EVENTS}/game_{game_id}_{ts}.ndjson", new_events)
            for e in new_events:
                self._seen_events[game_id][e["event_id"]] = e["event_type"]
            log.info("Game %d: wrote %d new/updated event(s).", game_id, len(new_events))

        # Player stats — always write latest snapshot
        boxscore = fetch_boxscore(self.session, game_id)
        if boxscore:
            stats = extract_player_stats(boxscore, game_id)
            if stats:
                _write_ndjson(f"{LANDING_STATS}/game_{game_id}_{ts}.ndjson", stats)
                log.info("Game %d: wrote %d stat record(s).", game_id, len(stats))

    def _poll_cycle(self) -> None:
        game_ids   = fetch_live_game_ids(self.session)
        current    = set(game_ids)
        just_ended = self._prev_live_ids - current

        for game_id in just_ended:
            log.info("Game %d just ended — final drain poll.", game_id)
            self._poll_game(game_id)

        self._prev_live_ids = current

        if not game_ids:
            log.debug("No live games.")
            return

        log.info("Live games: %s", game_ids)
        for game_id in game_ids:
            self._poll_game(game_id)

        self._save_checkpoint()

    def run(self) -> None:
        log.info("NHL producer started. Poll interval: %d s", self.poll_interval)
        while self._running:
            try:
                self._poll_cycle()
            except Exception as exc:
                log.exception("Unexpected error in poll cycle: %s", exc)
            time.sleep(self.poll_interval)
        log.info("Producer shut down cleanly.")


def main() -> None:
    cfg = load_config()
    NHLProducer(cfg).run()


if __name__ == "__main__":
    main()
