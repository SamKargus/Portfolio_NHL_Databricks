"""
great_expectations_setup.py  (GX 1.x API)
Creates expectation suites for Bronze, Silver, and Gold Delta tables.
Run once before data_quality_checks.py.
"""

import logging
import sys
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("ge_setup")

GE_DIR = Path(__file__).parents[2] / "great_expectations"
GE_DIR.mkdir(exist_ok=True)


def get_context():
    try:
        ctx = gx.get_context(context_root_dir=str(GE_DIR))
        log.info("Loaded existing GE context.")
    except Exception:
        ctx = gx.get_context(context_root_dir=str(GE_DIR), mode="file")
        log.info("Created new GE context at %s", GE_DIR)
    return ctx


def upsert_suite(ctx, name: str, expectations: list) -> None:
    """Add or replace an expectation suite."""
    try:
        suite = ctx.suites.get(name)
        log.info("Updating existing suite '%s'.", name)
    except Exception:
        suite = None

    if suite is None:
        suite = ctx.suites.add(gx.ExpectationSuite(name=name))
        log.info("Created suite '%s'.", name)

    # Clear existing expectations and re-add
    suite.expectations = expectations
    suite.save()
    log.info("Suite '%s' saved with %d expectation(s).", name, len(expectations))


# ── Bronze: game events ────────────────────────────────────────────────────────
def build_bronze_events_suite(ctx) -> None:
    upsert_suite(ctx, "bronze_game_events", [
        # Completeness
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="game_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_type"),
        gxe.ExpectColumnValuesToNotBeNull(column="kafka_timestamp"),
        # Uniqueness
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        # Domain checks
        gxe.ExpectColumnValuesToBeInSet(
            column="event_type",
            value_set=[
                "goal", "shot-on-goal", "penalty", "hit",
                "faceoff", "blocked-shot", "missed-shot",
                "giveaway", "takeaway",
            ],
        ),
        gxe.ExpectColumnValuesToBeBetween(column="period", min_value=1, max_value=9),
        # Row count sanity
        gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=5_000_000),
    ])


# ── Silver: game events ────────────────────────────────────────────────────────
def build_silver_events_suite(ctx) -> None:
    upsert_suite(ctx, "silver_game_events", [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="time_in_period_secs"),
        gxe.ExpectColumnValuesToNotBeNull(column="game_second"),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToBeBetween(
            column="time_in_period_secs", min_value=0, max_value=1200,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="game_second", min_value=0, max_value=10800,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="home_score", min_value=0, max_value=25, mostly=0.99,
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="away_score", min_value=0, max_value=25, mostly=0.99,
        ),
    ])


# ── Silver: player stats ───────────────────────────────────────────────────────
def build_silver_stats_suite(ctx) -> None:
    upsert_suite(ctx, "silver_player_stats", [
        gxe.ExpectColumnValuesToNotBeNull(column="player_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="game_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="team_id"),
        gxe.ExpectColumnValuesToBeInSet(
            column="position",
            value_set=["C", "L", "R", "D", "G"],
            mostly=0.99,
        ),
        gxe.ExpectColumnValuesToBeBetween(column="goals",   min_value=0, max_value=10),
        gxe.ExpectColumnValuesToBeBetween(column="assists", min_value=0, max_value=10),
        gxe.ExpectColumnValuesToBeBetween(column="shots",   min_value=0, max_value=20),
        gxe.ExpectColumnValuesToBeBetween(
            column="faceoff_pct", min_value=0, max_value=100, mostly=0.99,
        ),
    ])


# ── Gold: player scoring ───────────────────────────────────────────────────────
def build_gold_scoring_suite(ctx) -> None:
    upsert_suite(ctx, "gold_player_scoring", [
        gxe.ExpectColumnValuesToNotBeNull(column="player_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="game_id"),
        gxe.ExpectColumnValuesToBeBetween(column="goals",        min_value=0, max_value=10),
        gxe.ExpectColumnValuesToBeBetween(column="total_assists", min_value=0, max_value=10),
        gxe.ExpectColumnValuesToBeBetween(column="points",        min_value=0, max_value=20),
    ])


def main() -> None:
    ctx = get_context()
    build_bronze_events_suite(ctx)
    build_silver_events_suite(ctx)
    build_silver_stats_suite(ctx)
    build_gold_scoring_suite(ctx)
    log.info("All expectation suites created/updated successfully.")


if __name__ == "__main__":
    main()
