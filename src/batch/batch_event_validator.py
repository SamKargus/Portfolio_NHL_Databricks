"""
batch_event_validator.py

Distributed Spark job that validates NHL game events in the database against
the NHL API for one or more seasons, then backfills any missing or reclassified
events into both the bronze and silver PostgreSQL tables.

Each Spark partition owns a slice of game IDs, opens its own HTTP session and
DB connection, fetches play-by-play from the API, and writes differences
directly — no data is shuffled back to the driver until the final summary.

Usage
-----
    # Validate all seasons already in the DB
    python src/batch/batch_event_validator.py

    # Single season
    python src/batch/batch_event_validator.py --season 20242025

    # Multiple seasons
    python src/batch/batch_event_validator.py --seasons 20222023,20232024

    # Also walk the NHL schedule to discover games not yet in the DB
    python src/batch/batch_event_validator.py --season 20242025 --discover

    # Tune Spark parallelism
    python src/batch/batch_event_validator.py --partitions 16
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
)

sys.path.insert(0, str(Path(__file__).parents[2]))
from src.utils.db import PG_PARAMS, upsert_batch

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "batch_event_validator.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("event_validator")

# ── Constants ──────────────────────────────────────────────────────────────────
NHL_BASE = "https://api-web.nhle.com/v1"

RELEVANT_EVENT_TYPES = {
    "goal", "shot-on-goal", "penalty", "hit",
    "faceoff", "blocked-shot", "missed-shot", "giveaway", "takeaway",
}

# Seconds between API calls within each partition.
# With P partitions running in parallel: effective rate ≈ P / API_DELAY req/s
# Default 8 partitions × (1/1.0) = ~8 req/s = ~480 req/min (well under 600 limit)
API_DELAY_SECS = 1.0

# ── Schema for the per-game summary rows returned from each partition ──────────
RESULT_SCHEMA = StructType([
    StructField("game_id",          LongType(),    False),
    StructField("season",           IntegerType(), True),
    StructField("events_fetched",   IntegerType(), True),
    StructField("events_inserted",  IntegerType(), True),
    StructField("events_updated",   IntegerType(), True),
    StructField("api_error",        StringType(),  True),
])

# ── Bronze / Silver column lists for batch writes ─────────────────────────────
_BRONZE_COLS = [
    "event_id", "game_id", "season", "game_type", "event_type", "period",
    "period_type", "time_in_period", "time_remaining", "home_team_id",
    "home_team_abbrev", "away_team_id", "away_team_abbrev", "home_score",
    "away_score", "scoring_player_id", "assist1_player_id", "assist2_player_id",
    "goalie_in_net_id", "shot_type", "goal_type", "committed_by_player_id",
    "drawn_by_player_id", "penalty_type", "duration_minutes", "x_coord",
    "y_coord", "zone_code", "hittee_player_id", "hitting_player_id", "player_id",
]
_BRONZE_UPDATE_COLS = [
    "event_type", "home_score", "away_score", "period", "time_in_period",
    "scoring_player_id", "assist1_player_id", "assist2_player_id",
    "goalie_in_net_id", "shot_type", "goal_type", "penalty_type",
    "duration_minutes", "x_coord", "y_coord", "zone_code",
]

_SILVER_COLS = [
    "event_id", "game_id", "season", "game_type", "event_type",
    "period", "period_type", "time_in_period", "time_remaining",
    "time_in_period_secs", "time_remaining_secs", "game_second",
    "home_team_id", "home_team_abbrev", "away_team_id", "away_team_abbrev",
    "home_score", "away_score", "score_differential", "zone_code",
    "x_coord", "y_coord", "scoring_player_id", "scoring_player_total",
    "assist1_player_id", "assist1_player_total", "assist2_player_id",
    "assist2_player_total", "goalie_in_net_id", "shot_type", "goal_type",
    "penalty_type", "duration_minutes", "committed_by_player_id",
    "drawn_by_player_id", "hittee_player_id", "hitting_player_id", "player_id",
    "is_overtime", "is_penalty_shot",
]
_SILVER_UPDATE_COLS = [
    "event_type", "period", "time_in_period", "time_in_period_secs",
    "time_remaining_secs", "game_second", "home_score", "away_score",
    "score_differential", "zone_code", "x_coord", "y_coord",
    "scoring_player_id", "scoring_player_total", "assist1_player_id",
    "assist1_player_total", "assist2_player_id", "assist2_player_total",
    "goalie_in_net_id", "shot_type", "goal_type", "penalty_type",
    "duration_minutes", "is_overtime", "is_penalty_shot",
]


# ── API helpers ────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "NHL-EventValidator/1.0"
    return s


def _fetch_pbp(session: requests.Session, game_id: int) -> dict:
    url = f"{NHL_BASE}/gamecenter/{game_id}/play-by-play"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        return {"_error": str(exc)}


def _mm_ss(s: str) -> int:
    """'17:42' → 1062 seconds."""
    try:
        m, sec = s.split(":")
        return int(m) * 60 + int(sec)
    except Exception:
        return 0


def _extract_events(pbp: dict, game_id: int) -> list[dict]:
    """
    Parse play-by-play into event dicts with both bronze-layer fields and
    silver-layer derived fields (game_second, score_differential, etc.).
    Mirrors the logic in nhl_api_producer and silver_streaming.
    """
    home = pbp.get("homeTeam", {})
    away = pbp.get("awayTeam", {})
    events = []

    for play in pbp.get("plays", []):
        etype = play.get("typeDescKey", "").lower()
        if etype not in RELEVANT_EVENT_TYPES:
            continue

        d   = play.get("details", {})
        pd_ = play.get("periodDescriptor", {})

        period   = pd_.get("number")
        tip      = play.get("timeInPeriod", "")
        tip_secs = _mm_ss(tip)
        tr_secs  = _mm_ss(play.get("timeRemaining", ""))

        home_score = d.get("homeScore") if d.get("homeScore") is not None else home.get("score")
        away_score = d.get("awayScore") if d.get("awayScore") is not None else away.get("score")

        game_second = ((period - 1) * 1200 + tip_secs) if period else None
        score_diff  = (home_score - away_score) if (home_score is not None and away_score is not None) else None

        events.append({
            # ── identity ───────────────────────────────────────────────
            "event_id":               f"{game_id}_{play.get('eventId', 0)}",
            "game_id":                game_id,
            "season":                 pbp.get("season"),
            "game_type":              pbp.get("gameType"),
            # ── event classification ───────────────────────────────────
            "event_type":             etype,
            "period":                 period,
            "period_type":            pd_.get("periodType"),
            "time_in_period":         tip,
            "time_remaining":         play.get("timeRemaining"),
            # ── silver-derived timing fields ───────────────────────────
            "time_in_period_secs":    tip_secs,
            "time_remaining_secs":    tr_secs,
            "game_second":            game_second,
            # ── teams / score ──────────────────────────────────────────
            "home_team_id":           home.get("id"),
            "home_team_abbrev":       home.get("abbrev"),
            "away_team_id":           away.get("id"),
            "away_team_abbrev":       away.get("abbrev"),
            "home_score":             home_score,
            "away_score":             away_score,
            "score_differential":     score_diff,
            # ── goal details ───────────────────────────────────────────
            "scoring_player_id":      d.get("scoringPlayerId"),
            "scoring_player_total":   d.get("scoringPlayerTotal"),
            "assist1_player_id":      d.get("assist1PlayerId"),
            "assist1_player_total":   d.get("assist1PlayerTotal"),
            "assist2_player_id":      d.get("assist2PlayerId"),
            "assist2_player_total":   d.get("assist2PlayerTotal"),
            "goalie_in_net_id":       d.get("goalieInNetId"),
            "shot_type":              d.get("shotType"),
            "goal_type":              d.get("goalModifier"),
            # ── penalty details ────────────────────────────────────────
            "committed_by_player_id": d.get("committedByPlayerId"),
            "drawn_by_player_id":     d.get("drawnByPlayerId"),
            "penalty_type":           d.get("typeCode"),
            "duration_minutes":       d.get("duration"),
            # ── spatial ────────────────────────────────────────────────
            "x_coord":                d.get("xCoord"),
            "y_coord":                d.get("yCoord"),
            "zone_code":              d.get("zoneCode"),
            # ── hit / possession ───────────────────────────────────────
            "hittee_player_id":       d.get("hitteePlayerId"),
            "hitting_player_id":      d.get("hittingPlayerId"),
            "player_id":              d.get("playerId"),
            # ── silver flags ───────────────────────────────────────────
            "is_overtime":            bool(period and period > 3),
            "is_penalty_shot":        d.get("goalModifier") == "penalty-shot",
        })

    return events


# ── DB write helpers ───────────────────────────────────────────────────────────
def _pick(event: dict, cols: list) -> dict:
    return {c: event.get(c) for c in cols}


def _write_to_db(conn, events: list[dict]) -> None:
    """Upsert a batch of events into bronze and silver tables."""
    if not events:
        return
    with conn.cursor() as cur:
        upsert_batch(cur, "bronze_game_events",
                     [_pick(e, _BRONZE_COLS) for e in events],
                     conflict_cols=["event_id", "game_id"],
                     update_cols=_BRONZE_UPDATE_COLS)
        upsert_batch(cur, "silver_game_events",
                     [_pick(e, _SILVER_COLS) for e in events],
                     conflict_cols=["event_id", "game_id"],
                     update_cols=_SILVER_UPDATE_COLS)
    conn.commit()


# ── Spark partition function ───────────────────────────────────────────────────
def validate_partition(rows: Iterator[Row]) -> Iterator[Row]:
    """
    Runs inside each Spark executor. One function call per partition.

    Strategy:
    - Materialise all game_ids in the partition up-front so we can issue a
      single bulk SELECT against silver_game_events for the whole partition
      (one DB round-trip instead of one per game).
    - Then fetch each game from the NHL API with a per-request sleep to stay
      within the rate limit.
    - Write only the rows that are genuinely missing or reclassified.
    - Yield one summary Row per game for driver-side aggregation.
    """
    rows_list = list(rows)
    if not rows_list:
        return

    session = _make_session()
    conn    = psycopg2.connect(**PG_PARAMS)
    conn.autocommit = False

    # ── Bulk-load existing events for every game in this partition ─────────────
    # Shape: {game_id: {event_id: event_type}}
    db_state: dict[int, dict[str, str]] = {}
    game_ids = [int(r["game_id"]) for r in rows_list]
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT game_id, event_id, event_type
                FROM   silver_game_events
                WHERE  game_id = ANY(%s)
                """,
                (game_ids,),
            )
            for gid, eid, etype in cur.fetchall():
                db_state.setdefault(gid, {})[eid] = etype
    except Exception as exc:
        log.warning("Partition bulk-load failed (%s) — will treat all events as missing.", exc)
        try:
            conn.rollback()
        except Exception:
            pass

    # ── Process each game ──────────────────────────────────────────────────────
    try:
        for row in rows_list:
            game_id = int(row["game_id"])
            season  = row["season"]
            api_events: list[dict] = []
            inserted = updated = 0
            error: str | None = None

            try:
                time.sleep(API_DELAY_SECS)
                pbp = _fetch_pbp(session, game_id)

                if "_error" in pbp:
                    raise RuntimeError(pbp["_error"])

                api_events  = _extract_events(pbp, game_id)
                game_db     = db_state.get(game_id, {})
                to_write    = []

                for ev in api_events:
                    eid = ev["event_id"]
                    if eid not in game_db:
                        to_write.append(ev)
                        inserted += 1
                    elif game_db[eid] != ev["event_type"]:
                        # Event was reclassified (e.g. missed-shot → goal after review)
                        to_write.append(ev)
                        updated += 1

                _write_to_db(conn, to_write)

            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                error = str(exc)[:500]

            yield Row(
                game_id=game_id,
                season=season,
                events_fetched=len(api_events),
                events_inserted=inserted,
                events_updated=updated,
                api_error=error,
            )
    finally:
        conn.close()


# ── Game ID discovery ──────────────────────────────────────────────────────────
def get_db_game_ids(seasons: list[int] | None) -> list[tuple[int, int]]:
    """Return (game_id, season) for all games already tracked in silver."""
    conn = psycopg2.connect(**PG_PARAMS)
    try:
        with conn.cursor() as cur:
            if seasons:
                cur.execute(
                    """
                    SELECT DISTINCT game_id, season
                    FROM   silver_game_events
                    WHERE  season = ANY(%s)
                    ORDER  BY season, game_id
                    """,
                    (seasons,),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT game_id, season
                    FROM   silver_game_events
                    ORDER  BY season, game_id
                    """
                )
            return cur.fetchall()
    finally:
        conn.close()


# Approximate start/end dates for each season (used by --discover)
_SEASON_WINDOWS: dict[int, tuple[date, date]] = {
    20102011: (date(2010, 10, 7),  date(2011, 6, 30)),
    20112012: (date(2011, 10, 6),  date(2012, 6, 30)),
    20122013: (date(2013, 1, 19),  date(2013, 7, 1)),
    20132014: (date(2013, 10, 1),  date(2014, 6, 30)),
    20142015: (date(2014, 10, 8),  date(2015, 6, 30)),
    20152016: (date(2015, 10, 7),  date(2016, 6, 30)),
    20162017: (date(2016, 10, 12), date(2017, 6, 30)),
    20172018: (date(2017, 10, 4),  date(2018, 6, 30)),
    20182019: (date(2018, 10, 3),  date(2019, 7, 1)),
    20192020: (date(2019, 10, 2),  date(2020, 10, 1)),
    20202021: (date(2021, 1, 13),  date(2021, 7, 15)),
    20212022: (date(2021, 10, 12), date(2022, 7, 1)),
    20222023: (date(2022, 10, 7),  date(2023, 6, 26)),
    20232024: (date(2023, 10, 10), date(2024, 6, 30)),
    20242025: (date(2024, 10, 4),  date(2025, 7, 1)),
}


def discover_schedule_games(seasons: list[int]) -> list[tuple[int, int]]:
    """
    Walk the NHL schedule API week-by-week for each season to find game IDs
    that might not yet be in the database.  Returns (game_id, season) tuples.
    """
    session  = _make_session()
    seen     = set()
    games    = []

    for season in seasons:
        window = _SEASON_WINDOWS.get(season)
        if not window:
            log.warning("No known date window for season %d — skipping discovery.", season)
            continue

        log.info("Discovering season %d (%s → %s) …", season, window[0], window[1])
        current = window[0]

        while current <= window[1]:
            url = f"{NHL_BASE}/schedule/{current.isoformat()}"
            try:
                resp = session.get(url, timeout=10)
                resp.raise_for_status()
                for week in resp.json().get("gameWeek", []):
                    for game in week.get("games", []):
                        gtype = game.get("gameType", 0)
                        if gtype in (2, 3) and game["id"] not in seen:
                            seen.add(game["id"])
                            games.append((game["id"], season))
            except Exception as exc:
                log.warning("Schedule fetch failed for %s: %s", current, exc)

            time.sleep(0.5)
            current += timedelta(weeks=1)

    return games


# ── Spark session ──────────────────────────────────────────────────────────────
def _build_spark(partitions: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("NHL_Event_Validator")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.13:4.0.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", str(partitions))
        .getOrCreate()
    )


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate NHL event database against the NHL API and backfill gaps."
    )
    parser.add_argument("--season",     help="Single season code, e.g. 20242025")
    parser.add_argument("--seasons",    help="Comma-separated season codes")
    parser.add_argument("--game",       help="Single game ID to validate, e.g. 2025030114")
    parser.add_argument("--games",      help="Comma-separated game IDs to validate")
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Walk the NHL schedule to find games not yet in the DB (requires --season/--seasons)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="Spark parallelism — number of concurrent API workers (default: 8)",
    )
    args = parser.parse_args()

    # ── Resolve target seasons ─────────────────────────────────────────────────
    seasons: list[int] | None = None
    if args.season:
        seasons = [int(args.season)]
    elif args.seasons:
        seasons = [int(s.strip()) for s in args.seasons.split(",")]

    # ── If specific game IDs were given, bypass DB discovery entirely ──────────
    if args.game or args.games:
        raw_ids = args.games.split(",") if args.games else [args.game]
        explicit_game_ids = [int(g.strip()) for g in raw_ids]
        conn = psycopg2.connect(**PG_PARAMS)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT game_id, season FROM silver_game_events WHERE game_id = ANY(%s)",
                    (explicit_game_ids,),
                )
                known = {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
        # For any game not in DB yet, use season 0 as placeholder — validator still fetches it
        db_games = [(gid, known.get(gid, 0)) for gid in explicit_game_ids]
        log.info("Targeting %d explicit game(s): %s", len(db_games), explicit_game_ids)
    else:
        # ── Collect game IDs ───────────────────────────────────────────────────
        db_games = get_db_game_ids(seasons)
    log.info("Games from database:  %d", len(db_games))

    extra_games: list[tuple[int, int]] = []
    if args.discover:
        if not seasons:
            parser.error("--discover requires --season or --seasons")
        extra_games   = discover_schedule_games(seasons)
        db_set        = {gid for gid, _ in db_games}
        extra_games   = [(gid, s) for gid, s in extra_games if gid not in db_set]
        log.info("New games discovered: %d", len(extra_games))

    all_games = list({gid: s for gid, s in db_games + extra_games}.items())
    # all_games is [(game_id, season), ...]

    if not all_games:
        log.info("No games to validate.")
        return

    log.info("Total games to validate: %d across %d partition(s)", len(all_games), args.partitions)

    spark = _build_spark(args.partitions)

    # Accumulators for live progress logging from executors
    inserted_acc = spark.sparkContext.accumulator(0)
    updated_acc  = spark.sparkContext.accumulator(0)
    error_acc    = spark.sparkContext.accumulator(0)

    # ── Build input DataFrame and distribute ───────────────────────────────────
    games_df = (
        spark.createDataFrame(
            [Row(game_id=int(gid), season=int(s)) for gid, s in all_games],
        )
        .repartition(args.partitions, "game_id")
    )

    # ── Distributed validation: one partition = one API worker + DB writer ─────
    results_rdd = games_df.rdd.mapPartitions(validate_partition)
    results_df  = spark.createDataFrame(results_rdd, schema=RESULT_SCHEMA).cache()

    # Force evaluation (triggers all API calls and DB writes)
    total_games = results_df.count()

    # ── Aggregate summary ──────────────────────────────────────────────────────
    summary = results_df.agg(
        F.sum("events_fetched").alias("fetched"),
        F.sum("events_inserted").alias("inserted"),
        F.sum("events_updated").alias("updated"),
        F.count(F.when(F.col("api_error").isNotNull(), True)).alias("errored_games"),
    ).collect()[0]

    log.info(
        "Validation complete — games=%d  events_fetched=%d  inserted=%d  updated=%d  errors=%d",
        total_games,
        summary["fetched"]       or 0,
        summary["inserted"]      or 0,
        summary["updated"]       or 0,
        summary["errored_games"] or 0,
    )

    # Log top reclassifications for visibility
    reclassified = (
        results_df
        .filter(F.col("events_updated") > 0)
        .orderBy(F.col("events_updated").desc())
        .limit(20)
        .collect()
    )
    if reclassified:
        log.info("Games with reclassified events (top %d):", len(reclassified))
        for r in reclassified:
            log.info("  game_id=%-12d  season=%d  reclassified=%d",
                     r["game_id"], r["season"], r["events_updated"])

    # Log API errors
    errors = (
        results_df
        .filter(F.col("api_error").isNotNull())
        .select("game_id", "season", "api_error")
        .collect()
    )
    if errors:
        log.warning("%d game(s) had API errors:", len(errors))
        for r in errors[:20]:
            log.warning("  game_id=%d  season=%d  error=%s",
                        r["game_id"], r["season"], r["api_error"])
        if len(errors) > 20:
            log.warning("  … and %d more. See logs/batch_event_validator.log.", len(errors) - 20)

    spark.stop()


if __name__ == "__main__":
    main()
