"""
Live pipeline — Lakeflow (serverless, Continuous mode) source for NHL games in
progress. Started and stopped by src/live/live_poller.py around game times.

  Volume /Volumes/nhl/bronze/pbp_live_landing/<game_id>/<yyyyMMddTHHmmssZ>.json
     │  (one file per changed API fetch, written by live_poller.py)
     │  Auto Loader
     ▼
  nhl.bronze.nhl_pbp_live_raw     one row per fetch, raw JSON           (live bronze)
     ├─► nhl.bronze.nhl_pbp_live_plays   one row per play per fetch,    (live bronze,
     │                                   raw play JSON, append-only      play grain)
     └─► silver_live_sink (foreach_batch_sink)
            newest snapshot per game replaces that game's live rows
            (source = 'live') in all three silver tables:
            • nhl.silver.game_rosters
            • nhl.silver.nhl_plays
            • nhl.silver.games          (written last)

The sink reads nhl_pbp_live_raw (one row per file), not the plays table, so
every micro-batch holds complete snapshots — which is what makes replacing a
game's live rows with its newest snapshot correct (it also drops plays the NHL
removed). Rows are built by src/common/silver_parse.py, the same code the
nightly batch uses, so live and batch rows can't drift apart.

Once a game's silver row says source = 'batch' (the nightly run after it's
final), the sink leaves it alone — batch is the source of truth for finished
games. The pipeline writes nothing to gold: gold's fact_event and dim_game are
views over silver, so live games show up there straight away.

Pipeline settings
-----------------
  Source code   : this file only
  Catalog/schema: nhl / bronze
  Compute       : serverless
  Mode          : Continuous
  Configuration :
    nhl.live.src_common_path = /Workspace/<path to repo>/src/common   (required)
"""

import logging
from datetime import datetime

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

logger = logging.getLogger("live_pipeline")

LANDING_PATH    = "/Volumes/nhl/bronze/pbp_live_landing"
SRC_COMMON_PATH = spark.conf.get("nhl.live.src_common_path", "")
if not SRC_COMMON_PATH:
    raise ValueError(
        "Set pipeline configuration nhl.live.src_common_path to the workspace path "
        "of src/common (e.g. /Workspace/Users/<you>/Portfolio_NHL_Databricks/src/common)"
    )

# ----------------------------------------------------------------------
# Live bronze — raw snapshots via Auto Loader
# ----------------------------------------------------------------------
@dp.table(name="nhl_pbp_live_raw", comment="One row per NHL API fetch of a game in progress, raw JSON (live bronze).")
def nhl_pbp_live_raw():
    file_path = F.col("_metadata.file_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")          # whole file = one row, like nhl_events.raw_json
        .load(LANDING_PATH)
        .select(
            F.get_json_object("value", "$.id").cast("bigint").alias("game_id"),
            F.get_json_object("value", "$.gameState").alias("game_state"),
            F.to_timestamp(
                F.regexp_extract(file_path, r"(\d{8}T\d{6}Z)\.json$", 1),
                "yyyyMMdd'T'HHmmss'Z'",
            ).alias("fetched_at"),
            F.col("value").alias("raw_json"),
            file_path.alias("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# ----------------------------------------------------------------------
# Live bronze — play grain (every version of every play, raw play JSON)
# ----------------------------------------------------------------------
@dp.table(name="nhl_pbp_live_plays", comment="One row per play per fetch, raw play JSON (live bronze, play grain).")
def nhl_pbp_live_plays():
    return (
        spark.readStream.table("nhl_pbp_live_raw")
        .select(
            "game_id", "game_state", "fetched_at", "source_file",
            # array<string> keeps each play object as its raw JSON text
            F.explode(
                F.from_json(F.get_json_object("raw_json", "$.plays"), "array<string>")
            ).alias("play_json"),
        )
        .select(
            "game_id",
            F.get_json_object("play_json", "$.eventId").cast("int").alias("event_id"),
            F.get_json_object("play_json", "$.sortOrder").cast("int").alias("sort_order"),
            F.get_json_object("play_json", "$.typeDescKey").alias("type_desc_key"),
            "game_state",
            "fetched_at",
            "play_json",
            "source_file",
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )


# ----------------------------------------------------------------------
# Silver — written by a sink, since the pipeline doesn't own those tables
# ----------------------------------------------------------------------
@dp.foreach_batch_sink(name="silver_live_sink")
def write_live_snapshots(df, batch_id):
    # Imported here, not at module level: the sink function may be serialised
    # and run apart from this file, so it brings its own import path.
    import json
    import sys
    if SRC_COMMON_PATH not in sys.path:
        sys.path.insert(0, SRC_COMMON_PATH)
    from silver_parse import (
        GAMES_TABLE, ROSTERS_TABLE, PLAYS_TABLE,
        GAMES_SCHEMA, ROSTERS_SCHEMA, PLAYS_SCHEMA,
        parse_game, parse_roster, parse_plays,
    )

    session = df.sparkSession

    # 1. A micro-batch can hold several fetches of the same game — keep the newest.
    newest = Window.partitionBy("game_id").orderBy(F.col("fetched_at").desc())
    snapshots = (
        df.filter(F.col("game_id").isNotNull())
        .withColumn("_rn", F.row_number().over(newest))
        .filter("_rn = 1")
        .select("game_id", "fetched_at", "raw_json")
        .collect()                              # ≤ one row per game in progress
    )
    if not snapshots:
        return

    # 2. Skip games batch already owns, and snapshots older than what silver has
    #    (a late file). silver.games holds one row per game with its lineage.
    existing = {
        r.game_id: r
        for r in (
            session.table(GAMES_TABLE)
            .filter(F.col("game_id").isin([s.game_id for s in snapshots]))
            .select("game_id", "source", "fetched_at")
            .collect()
        )
    }

    def _apply(snap) -> bool:
        e = existing.get(snap.game_id)
        if e is None:
            return True
        if e.source == "batch":
            return False
        return e.fetched_at is None or snap.fetched_at >= e.fetched_at

    snapshots = [s for s in snapshots if _apply(s)]
    if not snapshots:
        return

    # 3. Build silver rows with the same parser the nightly batch uses.
    now = datetime.now()
    games, rosters, plays = [], [], []
    for snap in snapshots:
        data = json.loads(snap.raw_json)
        games.append(parse_game(data, snap.fetched_at, "live", now))
        rosters.extend(parse_roster(data, "live", now))
        plays.extend(parse_plays(data, "live", now))

    # 4. Replace each game's live rows with its newest snapshot. The snapshot
    #    is the game's complete state, so this inserts new plays, applies
    #    corrections and drops removed plays. Each table is one atomic commit;
    #    games goes last, so a game row means its rosters and plays are in.
    #    replaceWhere only touches these games' files (tables are clustered on
    #    game_id) and only rows with source = 'live'.
    ids_csv = ",".join(str(s.game_id) for s in snapshots)
    for rows, schema, table in [
        (rosters, ROSTERS_SCHEMA, ROSTERS_TABLE),
        (plays,   PLAYS_SCHEMA,   PLAYS_TABLE),
        (games,   GAMES_SCHEMA,   GAMES_TABLE),
    ]:
        (
            session.createDataFrame(rows, schema=schema)
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", f"game_id IN ({ids_csv}) AND source = 'live'")
            .saveAsTable(table)
        )
    logger.info(f"batch {batch_id}: wrote {len(games)} game(s), {len(plays)} plays to silver")


@dp.append_flow(target="silver_live_sink", name="silver_live_flow")
def silver_live_flow():
    return spark.readStream.table("nhl_pbp_live_raw")
