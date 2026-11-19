"""
Plan the parallel bronze backfill: split [start_date, end_date] into date
chunks of roughly equal work and hand them to the job's for-each task, which
runs batch_bronze.py once per chunk (see resources/nhl_batch_job.yml).

Chunks are contiguous and don't overlap (batch_bronze keeps only games dated
inside its own range), so together they cover every day exactly once and no
two tasks ever fetch the same game.

Work per day is estimated from the era: a 1920s season has ~200 games, a
modern one ~1,400, so equal-length date ranges would leave a few modern chunks
doing almost all the work. Each day also costs 1/7 of a schedule request
(the schedule endpoint returns a week per call), even when no games are played.

Job parameters (notebook widgets, or --name=value task arguments):
  start_date  first day to load, YYYY-MM-DD (default 1916-01-01)
  end_date    last day to load, YYYY-MM-DD; empty = today
  num_chunks  how many chunks to split the range into (default 16; fewer
              for short ranges, about one per MIN_CHUNK_DAYS days)

Output: task value `chunks`, a JSON list of
  {"chunk": 1, "start_date": "1916-01-01", "end_date": "1931-04-04"}
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

_here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
sys.path.insert(0, str(next(
    (p / "src" / "common" for p in [_here, *_here.parents] if (p / "src" / "common").is_dir()),
    _here,
)))
from job_params import get_param  # noqa: E402

DEFAULT_START = date(1916, 1, 1)
DEFAULT_CHUNKS = 16
# A short range (e.g. a nightly top-up of the last few days) isn't worth
# splitting: every chunk is a separate task with its own startup cost.
MIN_CHUNK_DAYS = 60
SCHEDULE_CALLS_PER_DAY = 1 / 7


def estimated_games(season: int) -> int:
    """Rough games per season (all game types) by era, for balancing only."""
    if season < 1942:
        return 200     # 6-10 teams, 24-48 game schedules
    if season < 1967:
        return 230     # Original Six
    if season < 1972:
        return 500     # first expansions
    if season < 1979:
        return 750     # 14-18 teams
    if season < 1991:
        return 900     # 21 teams
    if season < 2000:
        return 1150    # 22-28 teams
    if season < 2017:
        return 1350    # 30 teams
    return 1450        # 31-32 teams


def season_of(d: date) -> int:
    """NHL season by its start year; the season rolls over on September 1."""
    return d.year if d.month >= 9 else d.year - 1


def plan_chunks(start: date, end: date, num_chunks: int) -> list:
    """
    Split [start, end] into at most num_chunks contiguous ranges of ~equal
    estimated work (fewer when the range is short: ~MIN_CHUNK_DAYS per chunk).
    """
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    num_chunks = max(1, min(num_chunks, -(-len(days) // MIN_CHUNK_DAYS)))

    weights = [estimated_games(season_of(d)) / 365 + SCHEDULE_CALLS_PER_DAY for d in days]
    target = sum(weights) / num_chunks

    chunks = []
    chunk_start = start
    acc = 0.0
    for d, w in zip(days, weights):
        acc += w
        is_last_chunk = len(chunks) == num_chunks - 1
        if not is_last_chunk and d < end and acc >= target * (len(chunks) + 1):
            chunks.append((chunk_start, d))
            chunk_start = d + timedelta(days=1)
    chunks.append((chunk_start, end))

    return [
        {"chunk": i, "start_date": s.isoformat(), "end_date": e.isoformat()}
        for i, (s, e) in enumerate(chunks, 1)
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("plan_bronze_chunks")

    start = date.fromisoformat(get_param("start_date").strip() or DEFAULT_START.isoformat())
    end_param = get_param("end_date").strip()
    end = date.fromisoformat(end_param) if end_param else date.today()
    num_chunks = int(get_param("num_chunks").strip() or DEFAULT_CHUNKS)

    # Create / check the bronze table once here, before the chunk tasks start
    # appending to it in parallel.
    try:
        spark  # noqa: F821
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    from bronze_table import ensure_bronze_table
    ensure_bronze_table(spark, logger)

    chunks = plan_chunks(start, end, num_chunks)
    logger.info(f"Planned {len(chunks)} chunks for {start} → {end}:")
    for c in chunks:
        logger.info(f"  chunk {c['chunk']:>2}: {c['start_date']} → {c['end_date']}")

    from databricks.sdk.runtime import dbutils
    dbutils.jobs.taskValues.set(key="chunks", value=chunks)
    logger.info("Task value 'chunks' set for the bronze for-each task")
