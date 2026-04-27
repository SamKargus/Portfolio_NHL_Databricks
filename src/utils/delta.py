"""
delta.py — Delta Lake helpers for the NHL pipeline on Databricks.
Replaces the PostgreSQL db.py used in local development.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

DB_CATALOG = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
DB_SCHEMA  = os.getenv("DATABRICKS_SCHEMA",  "nhl")


def ensure_schema(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_CATALOG}.{DB_SCHEMA}")


def register_table(spark: SparkSession, table_name: str, path: str) -> None:
    """Register a Delta path in the metastore so Databricks SQL can query it by name."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DB_CATALOG}.{DB_SCHEMA}.{table_name}
        USING DELTA LOCATION '{path}'
    """)


def merge_into_delta(
    spark: SparkSession,
    path: str,
    batch_df: DataFrame,
    merge_keys: list[str],
    update_cols: list[str] | None = None,
) -> None:
    """
    Upsert batch_df into the Delta table at `path`.
    Creates the table (append) on first write; MERGEs on subsequent calls.

    update_cols=None  → update all columns on match.
    update_cols=[...] → update only the listed columns on match.
    """
    if batch_df.isEmpty():
        return

    if not DeltaTable.isDeltaTable(spark, path):
        batch_df.write.format("delta").mode("append").save(path)
        return

    target    = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    merger    = target.alias("target").merge(batch_df.alias("source"), condition)

    if update_cols:
        merger = merger.whenMatchedUpdate(set={c: f"source.{c}" for c in update_cols})
    else:
        merger = merger.whenMatchedUpdateAll()

    merger.whenNotMatchedInsertAll().execute()


def append_to_delta(batch_df: DataFrame, path: str) -> None:
    """Append-only write (for immutable snapshots like player stats)."""
    if batch_df.isEmpty():
        return
    batch_df.write.format("delta").mode("append").save(path)
