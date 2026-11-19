"""
Bronze game table shared by the batch scripts (batch_bronze.py and
plan_bronze_chunks.py).

The batch backfill runs many bronze tasks at once, each appending its own date
range to nhl.bronze.nhl_events. Delta lets concurrent blind appends commit
side by side, *except* on a table with an IDENTITY column: identity columns
disable concurrent transactions. So event_id is a plain BIGINT computed by the
writer instead (see make_event_id).
"""

from datetime import datetime

BRONZE_TABLE = "nhl.bronze.nhl_events"

BRONZE_SCHEMA = "event_id BIGINT, ingestion_timestamp TIMESTAMP, source STRING, raw_json STRING"


def make_event_id(game_id: int, fetched_at: datetime) -> int:
    """
    Unique, writer-generated bronze row id: game_id in the high 32 bits, unix
    seconds of the fetch in the low 32.

    * No coordination needed between parallel tasks: chunks never share a game.
    * Newer fetches of the same game get a bigger id, so batch_silver's "newest
      bronze row per game wins" ordering by event_id still holds.
    * Never collides with ids from the old IDENTITY column (1, 2, 3, ...):
      game ids start at 1917020001, so these ids are all above 2^62.
    * Fits a signed BIGINT: game ids stay below 2^31 and unix seconds below
      2^32 until 2106.
    """
    return (int(game_id) << 32) | (int(fetched_at.timestamp()) & 0xFFFFFFFF)


def ensure_bronze_table(spark, logger) -> None:
    """Create the bronze table if needed; refuse to run on the old IDENTITY layout."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            event_id BIGINT,
            ingestion_timestamp TIMESTAMP,
            source STRING,
            raw_json STRING
        ) USING DELTA
    """)

    identity = spark.sql("""
        SELECT 1 FROM nhl.information_schema.columns
        WHERE table_schema = 'bronze' AND table_name = 'nhl_events'
          AND column_name = 'event_id' AND is_identity = 'YES'
    """).collect()
    if identity:
        msg = (
            f"{BRONZE_TABLE}.event_id is still an IDENTITY column, which blocks the parallel "
            f"bronze tasks from writing at the same time. Copy the table without it (keeps every "
            f"row and id, so silver's processed_games tracker stays valid), then re-run:\n"
            f"  CREATE TABLE nhl.bronze.nhl_events_v2 AS SELECT * FROM {BRONZE_TABLE};\n"
            f"  DROP TABLE {BRONZE_TABLE};\n"
            f"  ALTER TABLE nhl.bronze.nhl_events_v2 RENAME TO {BRONZE_TABLE};"
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info(f"Table {BRONZE_TABLE} is ready")
