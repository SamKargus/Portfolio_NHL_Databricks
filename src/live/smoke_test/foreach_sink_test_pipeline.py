"""
Smoke test for the live path on Databricks Free Edition (serverless).

Answers three questions before we build the real live pipeline:
  1. Does Auto Loader in a serverless Lakeflow pipeline pick up game snapshot
     files from a Unity Catalog Volume?
  2. Is @dp.foreach_batch_sink (Public Preview) available, and can it MERGE
     into a table the pipeline does not own?
  3. Does the pipeline run in Continuous mode on Free Edition?

Pipeline settings
-----------------
  Source code : this file only
  Catalog     : nhl
  Schema      : scratch
  Compute     : serverless
  Mode        : Continuous (fall back to Triggered if Continuous is refused)

Drive it with foreach_sink_test_setup.py, which writes snapshot files into the
landing Volume step by step and checks the target table after each step.

Flow
----
  Volume (one JSON file per API fetch, <game_id>/<yyyyMMddTHHmmssZ>.json)
     │  Auto Loader
     ▼
  live_snapshots_raw   streaming table, one row per fetch (live bronze, raw)
     ├─► live_plays            streaming table, one row per play per fetch
     │                         (live bronze at play grain, append-only history)
     └─► silver_merge_sink     foreach_batch_sink → MERGE into TARGET_TABLE

The sink reads live_snapshots_raw (one row per file), not live_plays, so every
micro-batch holds complete snapshots. That is what makes it safe to delete
plays missing from a game's newest snapshot.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable

LANDING_PATH = "/Volumes/nhl/scratch/pbp_landing"
TARGET_TABLE = "nhl.scratch.plays_merge_target"   # stands in for nhl.silver.nhl_plays
FINAL_STATES = ["OFF", "FINAL"]

# ----------------------------------------------------------------------
# JSON schema — only the fields this test consumes
# ----------------------------------------------------------------------
_PLAY = T.StructType([
    T.StructField("eventId",          T.IntegerType()),
    T.StructField("sortOrder",        T.IntegerType()),
    T.StructField("typeDescKey",      T.StringType()),
    T.StructField("timeInPeriod",     T.StringType()),
    T.StructField("periodDescriptor", T.StructType([T.StructField("number", T.IntegerType())])),
    T.StructField("details", T.StructType([
        T.StructField("shotType",         T.StringType()),
        T.StructField("scoringPlayerId",  T.LongType()),
        T.StructField("shootingPlayerId", T.LongType()),
    ])),
])

_SNAPSHOT = T.StructType([
    T.StructField("id",        T.LongType()),
    T.StructField("gameState", T.StringType()),
    T.StructField("plays",     T.ArrayType(_PLAY)),
])


# ----------------------------------------------------------------------
# Parse helpers (shared by live_plays and the MERGE sink)
# ----------------------------------------------------------------------
def _parse_snapshots(raw):
    """One row per fetch: game_id, game_state, fetched_at, plays array."""
    return (
        raw
        .select("fetched_at", F.from_json("raw_json", _SNAPSHOT).alias("g"))
        .select(
            F.col("g.id").alias("game_id"),
            F.col("g.gameState").alias("game_state"),
            "fetched_at",
            F.col("g.plays").alias("plays"),
        )
        .filter(F.col("game_id").isNotNull())
    )


def _explode_plays(snapshots, source):
    """One row per play. Column order matches TARGET_TABLE exactly."""
    return (
        snapshots
        .select("game_id", "game_state", "fetched_at", F.explode("plays").alias("p"))
        .select(
            F.col("game_id"),
            F.col("p.eventId").alias("event_id"),
            F.col("p.sortOrder").alias("sort_order"),
            F.col("p.typeDescKey").alias("type_desc_key"),
            F.col("p.periodDescriptor.number").alias("period_number"),
            F.col("p.timeInPeriod").alias("time_in_period"),
            F.col("p.details.shotType").alias("shot_type"),
            F.col("p.details.scoringPlayerId").alias("scoring_player_id"),
            F.col("p.details.shootingPlayerId").alias("shooting_player_id"),
            F.col("game_state"),
            F.col("game_state").isin(FINAL_STATES).alias("is_final"),
            F.lit(source).alias("source"),
            F.col("fetched_at"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
        .filter(F.col("event_id").isNotNull())
    )


# ----------------------------------------------------------------------
# Live bronze — raw snapshots via Auto Loader
# ----------------------------------------------------------------------
@dp.table(name="live_snapshots_raw", comment="One row per NHL API fetch, raw JSON (live bronze).")
def live_snapshots_raw():
    file_path = F.col("_metadata.file_path")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")          # whole file = one row, like nhl_events.raw_json
        .load(LANDING_PATH)
        .select(
            F.get_json_object("value", "$.id").cast("bigint").alias("game_id"),
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
# Live bronze — play grain (append-only history of every play version)
# ----------------------------------------------------------------------
@dp.table(name="live_plays", comment="One row per play per fetch (live bronze, play grain).")
def live_plays():
    return _explode_plays(_parse_snapshots(spark.readStream.table("live_snapshots_raw")), "live")


# ----------------------------------------------------------------------
# Silver — MERGE into a table the pipeline does not own
# ----------------------------------------------------------------------
@dp.foreach_batch_sink(name="silver_merge_sink")
def merge_into_silver(df, batch_id):
    session = df.sparkSession
    target = DeltaTable.forName(session, TARGET_TABLE)

    # 1. A micro-batch can hold several fetches of the same game; MERGE fails
    #    if two source rows match one target row, so keep the newest per game.
    newest = Window.partitionBy("game_id").orderBy(F.col("fetched_at").desc())
    latest = (
        _parse_snapshots(df)
        .withColumn("_rn", F.row_number().over(newest))
        .filter("_rn = 1")
        .drop("_rn")
    )

    batch_game_ids = [r.game_id for r in latest.select("game_id").collect()]
    if not batch_game_ids:
        return

    # 2. Drop snapshots that are older than what silver already has (a late
    #    file), and games batch has already loaded — batch is the source of
    #    truth once a game is final.
    existing = (
        target.toDF()
        .filter(F.col("game_id").isin(batch_game_ids))
        .groupBy("game_id")
        .agg(
            F.max(F.when(F.col("source") == "live", F.col("fetched_at"))).alias("_live_fetched_at"),
            F.max((F.col("source") == "batch").cast("int")).alias("_has_batch"),
        )
    )
    latest = (
        latest.join(existing, "game_id", "left")
        .filter(
            F.col("_has_batch").isNull() | (F.col("_has_batch") == 0)
        )
        .filter(
            F.col("_live_fetched_at").isNull()
            | (F.col("fetched_at") >= F.col("_live_fetched_at"))
        )
        .drop("_live_fetched_at", "_has_batch")
    )

    game_ids = [r.game_id for r in latest.select("game_id").collect()]
    if not game_ids:
        return
    game_id_list = ",".join(str(g) for g in game_ids)

    # 3. Upsert plays, and delete live plays the NHL removed — scoped to the
    #    games in this batch so other games and batch rows are never touched.
    (
        target.alias("t")
        .merge(
            _explode_plays(latest, "live").alias("s"),
            "t.game_id = s.game_id AND t.event_id = s.event_id",
        )
        .whenMatchedUpdateAll(condition="t.source = 'live'")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(
            condition=f"t.game_id IN ({game_id_list}) AND t.source = 'live'"
        )
        .execute()
    )


@dp.append_flow(target="silver_merge_sink", name="silver_merge_flow")
def silver_merge_flow():
    return spark.readStream.table("live_snapshots_raw")
