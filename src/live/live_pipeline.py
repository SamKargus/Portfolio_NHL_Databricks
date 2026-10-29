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
            • nhl.silver.nhl_plays   newest snapshot per game replaces that
                                     game's live rows (source = 'live')
            • nhl.gold.dim_game      upsert, so live plays join to a game row
            • nhl.gold.dim_player    insert debut players only

The sink reads nhl_pbp_live_raw (one row per file), not the plays table, so
every micro-batch holds complete snapshots — which is what makes replacing a
game's live rows with its newest snapshot correct (it also drops plays the NHL
removed). Rows are built by src/common/silver_parse.py, the same code the
nightly batch uses, so live and batch rows can't drift apart.

Once a game has batch rows in silver (the nightly run after it's final), the
sink leaves it alone — batch is the source of truth for finished games.

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
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable

logger = logging.getLogger("live_pipeline")

LANDING_PATH    = "/Volumes/nhl/bronze/pbp_live_landing"
SRC_COMMON_PATH = spark.conf.get("nhl.live.src_common_path", "")
if not SRC_COMMON_PATH:
    raise ValueError(
        "Set pipeline configuration nhl.live.src_common_path to the workspace path "
        "of src/common (e.g. /Workspace/Users/<you>/Portfolio_NHL_Databricks/src/common)"
    )

DIM_GAME_TABLE   = "nhl.gold.dim_game"
DIM_PLAYER_TABLE = "nhl.gold.dim_player"

# Column types match build_dim_game / build_dim_player in batch_gold.py
DIM_GAME_SCHEMA = T.StructType([
    T.StructField("game_id",             T.LongType()),
    T.StructField("season",              T.IntegerType()),
    T.StructField("game_type",           T.IntegerType()),
    T.StructField("game_date",           T.DateType()),
    T.StructField("date_key",            T.IntegerType()),
    T.StructField("start_time_utc",      T.TimestampType()),
    T.StructField("venue",               T.StringType()),
    T.StructField("venue_location",      T.StringType()),
    T.StructField("game_state",          T.StringType()),
    T.StructField("game_schedule_state", T.StringType()),
    T.StructField("limited_scoring",     T.BooleanType()),
    T.StructField("reg_periods",         T.IntegerType()),
    T.StructField("last_period_type",    T.StringType()),
    T.StructField("home_team_id",        T.IntegerType()),
    T.StructField("away_team_id",        T.IntegerType()),
    T.StructField("home_final_score",    T.IntegerType()),
    T.StructField("away_final_score",    T.IntegerType()),
    T.StructField("home_final_sog",      T.IntegerType()),
    T.StructField("away_final_sog",      T.IntegerType()),
    T.StructField("ingestion_timestamp", T.TimestampType()),
])

DIM_PLAYER_SCHEMA = T.StructType([
    T.StructField("player_id",           T.LongType()),
    T.StructField("full_name",           T.StringType()),
    T.StructField("first_name",          T.StringType()),
    T.StructField("last_name",           T.StringType()),
    T.StructField("position_code",       T.StringType()),
    T.StructField("sweater_number",      T.IntegerType()),
    T.StructField("team_id",             T.IntegerType()),
    T.StructField("headshot_url",        T.StringType()),
    T.StructField("first_seen_date",     T.DateType()),
    T.StructField("last_seen_date",      T.DateType()),
    T.StructField("ingestion_timestamp", T.TimestampType()),
])


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
# Silver + gold dims — written by a sink, since the pipeline doesn't own them
# ----------------------------------------------------------------------
def _dim_game_row(ctx: dict) -> dict:
    row = {f.name: ctx.get(f.name) for f in DIM_GAME_SCHEMA.fields}
    row["date_key"] = int(ctx["game_date"].strftime("%Y%m%d")) if ctx.get("game_date") else None
    return row


def _dim_player_rows(data: dict, game_date, ingestion_ts: datetime) -> list:
    rows = []
    for spot in data.get("rosterSpots") or []:
        if not spot.get("playerId"):
            continue
        first = (spot.get("firstName") or {}).get("default")
        last  = (spot.get("lastName") or {}).get("default")
        rows.append({
            "player_id":           int(spot["playerId"]),
            "full_name":           " ".join(p for p in (first, last) if p),
            "first_name":          first,
            "last_name":           last,
            "position_code":       spot.get("positionCode"),
            "sweater_number":      spot.get("sweaterNumber"),
            "team_id":             spot.get("teamId"),
            "headshot_url":        spot.get("headshot"),
            "first_seen_date":     game_date,
            "last_seen_date":      game_date,
            "ingestion_timestamp": ingestion_ts,
        })
    return rows


@dp.foreach_batch_sink(name="silver_live_sink")
def write_live_snapshots(df, batch_id):
    # Imported here, not at module level: the sink function may be serialised
    # and run apart from this file, so it brings its own import path.
    import json
    import sys
    if SRC_COMMON_PATH not in sys.path:
        sys.path.insert(0, SRC_COMMON_PATH)
    from silver_parse import SILVER_TABLE, SILVER_SCHEMA, parse_game_context, parse_plays, player_names

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
    #    (a late file).
    game_ids = [r.game_id for r in snapshots]
    existing = {
        r.game_id: r
        for r in (
            session.table(SILVER_TABLE)
            .filter(F.col("game_id").isin(game_ids))
            .groupBy("game_id")
            .agg(
                F.max(F.when(F.col("source") == "live", F.col("fetched_at"))).alias("live_fetched_at"),
                F.max((F.col("source") == "batch").cast("int")).alias("has_batch"),
            )
            .collect()
        )
    }

    def _apply(snap) -> bool:
        e = existing.get(snap.game_id)
        if e is None:
            return True
        if e.has_batch:
            return False
        return e.live_fetched_at is None or snap.fetched_at >= e.live_fetched_at

    snapshots = [s for s in snapshots if _apply(s)]
    if not snapshots:
        return

    # 3. Build silver rows with the same parser the nightly batch uses.
    now = datetime.now()
    play_rows, game_rows, player_rows = [], [], {}
    for snap in snapshots:
        data = json.loads(snap.raw_json)
        play_rows.extend(parse_plays(data, snap.fetched_at, player_names(data), "live", now))
        ctx = parse_game_context(data, snap.fetched_at, "live", now)
        game_rows.append(_dim_game_row(ctx))
        for p in _dim_player_rows(data, ctx.get("game_date"), now):
            player_rows[p["player_id"]] = p

    # 4. Replace each game's live rows with its newest snapshot, atomically.
    #    The snapshot is the game's complete play list, so this inserts new
    #    plays, applies corrections and drops removed plays in one commit.
    #    replaceWhere only touches these games' files (silver is clustered on
    #    game_id) and only rows with source = 'live'.
    ids_csv = ",".join(str(s.game_id) for s in snapshots)
    (
        session.createDataFrame(play_rows, schema=SILVER_SCHEMA)
        .write.format("delta").mode("overwrite")
        .option("replaceWhere", f"game_id IN ({ids_csv}) AND source = 'live'")
        .saveAsTable(SILVER_TABLE)
    )

    # 5. Keep the dimensions joinable for games in progress. The nightly
    #    batch_gold overwrites both tables from bronze, replacing these rows.
    try:
        (
            DeltaTable.forName(session, DIM_GAME_TABLE).alias("t")
            .merge(session.createDataFrame(game_rows, schema=DIM_GAME_SCHEMA).alias("s"),
                   "t.game_id = s.game_id")
            .whenMatchedUpdateAll(condition="t.game_state NOT IN ('OFF', 'FINAL')")  # never touch batch rows
            .whenNotMatchedInsertAll()
            .execute()
        )
        if player_rows:
            (
                DeltaTable.forName(session, DIM_PLAYER_TABLE).alias("t")
                .merge(session.createDataFrame(list(player_rows.values()), schema=DIM_PLAYER_SCHEMA).alias("s"),
                       "t.player_id = s.player_id")
                .whenNotMatchedInsertAll()              # debut players only
                .execute()
            )
    except Exception as e:  # e.g. gold not built yet — silver is what matters
        logger.warning(f"batch {batch_id}: could not update gold dims: {e}")


@dp.append_flow(target="silver_live_sink", name="silver_live_flow")
def silver_live_flow():
    return spark.readStream.table("nhl_pbp_live_raw")
