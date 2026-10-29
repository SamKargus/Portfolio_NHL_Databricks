# Databricks notebook source
# MAGIC %md
# MAGIC # Live path smoke test — driver notebook
# MAGIC
# MAGIC Drives `foreach_sink_test_pipeline.py` by writing fake "live" snapshots of a real,
# MAGIC finished game into the landing Volume, one step at a time, and checking the MERGE
# MAGIC target after each step.
# MAGIC
# MAGIC | Step | Writes | Proves |
# MAGIC |------|--------|--------|
# MAGIC | A | first 100 plays, `LIVE` | Auto Loader + sink insert work |
# MAGIC | B | first 200 plays, one shot corrected, one play removed | update + scoped delete |
# MAGIC | C | a *late* older snapshot, and a live snapshot for a game batch already owns | both are ignored |
# MAGIC | D | all plays, `OFF` | game goes final, removed play comes back |
# MAGIC
# MAGIC **Run order**
# MAGIC 1. Run cells 1–3 (setup + step A files).
# MAGIC 2. Create the pipeline: source = `foreach_sink_test_pipeline.py`, catalog `nhl`, schema `scratch`,
# MAGIC    serverless, **Continuous** mode. Start it. If Continuous is refused, switch to Triggered and
# MAGIC    click *Start* after writing each step's files instead — and note that it was refused.
# MAGIC 3. Run each step's "write" cell, then its "check" cell.
# MAGIC 4. Teardown cell at the bottom when finished.

# COMMAND ----------

# 1. Config + scratch objects
import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests

CATALOG_SCHEMA = "nhl.scratch"
LANDING_PATH   = "/Volumes/nhl/scratch/pbp_landing"
TARGET_TABLE   = "nhl.scratch.plays_merge_target"
RAW_TABLE      = "nhl.scratch.live_snapshots_raw"
PLAYS_TABLE    = "nhl.scratch.live_plays"

TEST_GAME_ID   = 2024020500   # finished game, 2024-12-17, 323 plays
BATCH_GAME_ID  = 1            # decoy: a game batch already owns
OTHER_LIVE_ID  = 2            # decoy: a different live game

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_SCHEMA}.pbp_landing")

# Column order must match _explode_plays() in the pipeline
spark.sql(f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE} (
        game_id             BIGINT,
        event_id            INT,
        sort_order          INT,
        type_desc_key       STRING,
        period_number       INT,
        time_in_period      STRING,
        shot_type           STRING,
        scoring_player_id   BIGINT,
        shooting_player_id  BIGINT,
        game_state          STRING,
        is_final            BOOLEAN,
        source              STRING,
        fetched_at          TIMESTAMP,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# Decoys — the sink must never touch these
spark.sql(f"""
    INSERT INTO {TARGET_TABLE} VALUES
      ({BATCH_GAME_ID}, 1, 1, 'decoy-batch', 1, '00:00', NULL, NULL, NULL, 'OFF',  true,  'batch', NULL,                    current_timestamp()),
      ({OTHER_LIVE_ID}, 1, 1, 'decoy-live',  1, '00:00', NULL, NULL, NULL, 'LIVE', false, 'live',  current_timestamp(),     current_timestamp())
""")
print("Scratch schema, volume and target table ready.")

# COMMAND ----------

# 2. Fetch the real game and define the snapshot writer
resp = requests.get(f"https://api-web.nhle.com/v1/gamecenter/{TEST_GAME_ID}/play-by-play", timeout=30)
resp.raise_for_status()
FULL_GAME = resp.json()
ALL_PLAYS = FULL_GAME["plays"]
print(f"Fetched game {TEST_GAME_ID}: {len(ALL_PLAYS)} plays, state={FULL_GAME['gameState']}")


def write_snapshot(game_id: int, plays: list, game_state: str, fetched_at: datetime) -> str:
    """Write one fake API fetch to <landing>/<game_id>/<yyyyMMddTHHmmssZ>.json."""
    payload = {**FULL_GAME, "id": game_id, "gameState": game_state, "plays": plays}
    path = f"{LANDING_PATH}/{game_id}/{fetched_at:%Y%m%dT%H%M%SZ}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(payload, f)
    print(f"Wrote {path} ({len(plays)} plays, {game_state})")
    return path


def now_utc() -> datetime:
    time.sleep(1.1)  # filenames have 1-second resolution — keep them unique
    return datetime.now(timezone.utc)


def wait_for_raw_rows(n: int, timeout_s: int = 180, settle_s: int = 20) -> None:
    """Wait until Auto Loader has ingested n files, then give the sink time to MERGE."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            got = spark.table(RAW_TABLE).count()
        except Exception:
            got = 0  # table not created until the pipeline's first update
        if got >= n:
            time.sleep(settle_s)
            print(f"{RAW_TABLE} has {got} rows")
            return
        time.sleep(5)
    raise TimeoutError(f"{RAW_TABLE} did not reach {n} rows in {timeout_s}s — is the pipeline running?")


def test_game():
    return spark.table(TARGET_TABLE).filter(f"game_id = {TEST_GAME_ID}")


def check_decoys() -> None:
    rows = {r.game_id: r for r in spark.table(TARGET_TABLE).filter(f"game_id IN ({BATCH_GAME_ID}, {OTHER_LIVE_ID})").collect()}
    assert rows.get(BATCH_GAME_ID) and rows[BATCH_GAME_ID].type_desc_key == "decoy-batch", "batch decoy was modified or deleted"
    assert rows.get(OTHER_LIVE_ID) and rows[OTHER_LIVE_ID].type_desc_key == "decoy-live", "other live game was modified or deleted"
    assert spark.table(TARGET_TABLE).filter(f"game_id = {BATCH_GAME_ID}").count() == 1, "live rows were inserted into a batch-owned game"
    print("Decoys untouched ✔")


# Plays used by the update/delete steps
CORRECTED_IDX = next(i for i, p in enumerate(ALL_PLAYS[:200]) if p["typeDescKey"] == "shot-on-goal")
REMOVED_IDX   = 50 if CORRECTED_IDX != 50 else 51
CORRECTED_EVENT_ID = ALL_PLAYS[CORRECTED_IDX]["eventId"]
REMOVED_EVENT_ID   = ALL_PLAYS[REMOVED_IDX]["eventId"]
ORIGINAL_SHOT_TYPE = ALL_PLAYS[CORRECTED_IDX]["details"].get("shotType")
print(f"Will correct eventId={CORRECTED_EVENT_ID} and remove eventId={REMOVED_EVENT_ID}")

# COMMAND ----------

# 3. Step A — write: first 100 plays, LIVE  (then create + start the pipeline)
write_snapshot(TEST_GAME_ID, ALL_PLAYS[:100], "LIVE", now_utc())

# COMMAND ----------

# 3. Step A — check
wait_for_raw_rows(1)
assert test_game().count() == 100, f"expected 100 plays, got {test_game().count()}"
assert test_game().filter("source = 'live' AND NOT is_final").count() == 100
check_decoys()
print("Step A passed ✔  Auto Loader + foreach_batch_sink MERGE insert work")

# COMMAND ----------

# 4. Step B — write: first 200 plays, one shot-type corrected, one play removed
plays_b = [json.loads(json.dumps(p)) for p in ALL_PLAYS[:200]]   # deep copy
plays_b[CORRECTED_IDX]["details"]["shotType"] = "corrected-in-test"
del plays_b[REMOVED_IDX]
SNAP_B_TIME = now_utc()
write_snapshot(TEST_GAME_ID, plays_b, "LIVE", SNAP_B_TIME)

# COMMAND ----------

# 4. Step B — check
wait_for_raw_rows(2)
assert test_game().count() == 199, f"expected 199 plays, got {test_game().count()}"
assert test_game().filter(f"event_id = {CORRECTED_EVENT_ID}").first().shot_type == "corrected-in-test", "update did not apply"
assert test_game().filter(f"event_id = {REMOVED_EVENT_ID}").count() == 0, "removed play was not deleted"
check_decoys()
print("Step B passed ✔  updates and game-scoped deletes work")

# COMMAND ----------

# 5. Step C — write: a late (older) snapshot, and a live snapshot for a batch-owned game
write_snapshot(TEST_GAME_ID, ALL_PLAYS[:20], "LIVE", SNAP_B_TIME - timedelta(minutes=5))
write_snapshot(BATCH_GAME_ID, ALL_PLAYS[:3], "LIVE", now_utc())

# COMMAND ----------

# 5. Step C — check (nothing in the target should change)
wait_for_raw_rows(4)
assert test_game().count() == 199, f"late snapshot was applied: {test_game().count()} plays"
assert test_game().filter(f"event_id = {CORRECTED_EVENT_ID}").first().shot_type == "corrected-in-test"
check_decoys()
print("Step C passed ✔  late files and batch-owned games are ignored")

# COMMAND ----------

# 6. Step D — write: the full game, OFF
write_snapshot(TEST_GAME_ID, ALL_PLAYS, "OFF", now_utc())

# COMMAND ----------

# 6. Step D — check
wait_for_raw_rows(5)
n = len(ALL_PLAYS)
assert test_game().count() == n, f"expected {n} plays, got {test_game().count()}"
assert test_game().filter("is_final").count() == n, "not every play is marked final"
assert test_game().filter(f"event_id = {CORRECTED_EVENT_ID}").first().shot_type == ORIGINAL_SHOT_TYPE
assert test_game().filter(f"event_id = {REMOVED_EVENT_ID}").count() == 1, "removed play did not come back"
check_decoys()

expected_play_rows = 100 + 199 + 20 + 3 + n
got_play_rows = spark.table(PLAYS_TABLE).count()
assert got_play_rows == expected_play_rows, f"{PLAYS_TABLE}: expected {expected_play_rows}, got {got_play_rows}"
print(f"Step D passed ✔  final snapshot applied; {PLAYS_TABLE} kept all {got_play_rows} play versions")
print("\nALL STEPS PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown
# MAGIC Stop and delete the pipeline in the UI first, then run the cell below.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_SCHEMA} CASCADE")   # drops tables + the landing volume
