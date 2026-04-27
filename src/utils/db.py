"""
db.py — PostgreSQL connection helpers for the NHL pipeline.
Reads credentials from environment variables (set via config/.env).
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# ── Connection params (override via env) ───────────────────────────────────────
PG_PARAMS = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "nhl_pipeline"),
    user=os.getenv("PG_USER", os.getenv("USER", "samkargus")),
    password=os.getenv("PG_PASSWORD", ""),
)


@contextmanager
def get_conn():
    """Yield a psycopg2 connection; commit and close on exit."""
    conn = psycopg2.connect(**PG_PARAMS)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_batch(cur, table: str, rows: list[dict], conflict_cols: list[str], update_cols: list[str]) -> None:
    """
    Generic INSERT ... ON CONFLICT DO UPDATE for a list of row dicts.
    All rows must have the same keys.
    """
    if not rows:
        return
    cols = list(rows[0].keys())
    col_str = ", ".join(cols)
    val_str = ", ".join(f"%({c})s" for c in cols)
    conflict_str = ", ".join(conflict_cols)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({col_str})
        VALUES ({val_str})
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
    """
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)


def insert_ignore_batch(cur, table: str, rows: list[dict]) -> None:
    """INSERT ... ON CONFLICT DO NOTHING for deduplication."""
    if not rows:
        return
    cols = list(rows[0].keys())
    col_str = ", ".join(cols)
    val_str = ", ".join(f"%({c})s" for c in cols)

    sql = f"""
        INSERT INTO {table} ({col_str})
        VALUES ({val_str})
        ON CONFLICT DO NOTHING
    """
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
