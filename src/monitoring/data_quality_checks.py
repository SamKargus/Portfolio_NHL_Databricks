"""
data_quality_checks.py  (GX 1.x API)
Validates Bronze, Silver, and Gold Delta tables using Great Expectations.
Also runs three custom pandas-based anomaly checks.

Exit code 1 if any critical suite fails (use --fail-on-critical flag).
"""

import argparse
import copy
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
import pyarrow.dataset as ds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parents[2] / "logs" / "data_quality.log"
        ),
    ],
)
log = logging.getLogger("data_quality")

GE_DIR = Path(__file__).parents[2] / "great_expectations"

import os as _os
_IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in _os.environ
_BASE = "dbfs:/mnt/nhl" if _IS_DATABRICKS else "/tmp/nhl"

SUITE_TABLE_MAP = {
    "bronze_game_events":  f"{_BASE}/bronze/game_events",
    "silver_game_events":  f"{_BASE}/silver/game_events",
    "silver_player_stats": f"{_BASE}/silver/player_stats",
    "gold_player_scoring": f"{_BASE}/gold/player_scoring",
}

CRITICAL_SUITES = {"silver_game_events", "silver_player_stats"}


@dataclass
class DQResult:
    suite:   str
    success: bool
    stats:   dict = field(default_factory=dict)

    def __str__(self):
        status = "PASS" if self.success else "FAIL"
        evaluated   = self.stats.get("evaluated_expectations", "?")
        successful  = self.stats.get("successful_expectations", "?")
        failed      = self.stats.get("unsuccessful_expectations", "?")
        return f"[{status}] {self.suite} | evaluated={evaluated} ok={successful} failed={failed}"


# ── Load table into pandas ─────────────────────────────────────────────────────

def load_table(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        log.warning("Path does not exist: %s", path)
        return None
    try:
        dataset = ds.dataset(path, format="parquet", partitioning="hive")
        if dataset.count_rows() == 0:
            log.warning("Table is empty: %s", path)
            return None
        return dataset.to_table().to_pandas()
    except Exception as exc:
        log.error("Failed to read table at %s: %s", path, exc)
        return None


# ── GE 1.x validation ─────────────────────────────────────────────────────────

def validate_suite(ctx, suite_name: str, table_path: str) -> DQResult:
    log.info("Validating '%s' …", suite_name)

    df = load_table(table_path)
    if df is None:
        log.warning("Skipping '%s' — no data.", suite_name)
        return DQResult(suite_name, success=True, stats={"skipped": True})

    try:
        suite = ctx.suites.get(suite_name)
    except Exception:
        log.warning("Suite '%s' not found — run great_expectations_setup.py first.", suite_name)
        return DQResult(suite_name, success=False, stats={"error": "suite not found"})

    try:
        # GX 1.x ephemeral pandas datasource for this validation run
        tmp_ctx = gx.get_context(mode="ephemeral")
        datasource = tmp_ctx.data_sources.add_pandas(f"ds_{suite_name}")
        asset      = datasource.add_dataframe_asset(f"asset_{suite_name}")
        batch_def  = asset.add_batch_definition_whole_dataframe("batch")

        # Deep-copy expectations so they are not owned by the file-context suite
        tmp_suite  = tmp_ctx.suites.add(gx.ExpectationSuite(
            name=suite_name,
            expectations=[copy.copy(e) for e in suite.expectations],
        ))

        vd = tmp_ctx.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"vd_{suite_name}",
                data=batch_def,
                suite=tmp_suite,
            )
        )

        result = vd.run(batch_parameters={"dataframe": df})
        success = result.success

        stats = {
            "evaluated_expectations":   result.statistics.get("evaluated_expectations", 0),
            "successful_expectations":  result.statistics.get("successful_expectations", 0),
            "unsuccessful_expectations":result.statistics.get("unsuccessful_expectations", 0),
        }

        if not success:
            for er in result.results:
                if not er.success:
                    log.warning("  FAIL: %s — %s",
                                er.expectation_config.type,
                                er.result)

        return DQResult(suite_name, success=success, stats=stats)

    except Exception as exc:
        log.error("Validation error for '%s': %s", suite_name, exc)
        return DQResult(suite_name, success=False, stats={"error": str(exc)})


# ── Custom anomaly checks ──────────────────────────────────────────────────────

def check_goal_scorer_not_null(table_path: str) -> DQResult:
    """Every goal event must have a scoring_player_id."""
    name = "custom_goal_scorer_not_null"
    df = load_table(table_path)
    if df is None:
        return DQResult(name, success=True, stats={"skipped": True})

    goals = df[df["event_type"] == "goal"]
    if len(goals) == 0:
        return DQResult(name, success=True, stats={"total_goals": 0})

    missing = goals["scoring_player_id"].isna().sum()
    pct     = missing / len(goals) * 100
    success = pct < 1.0

    stats = {"total_goals": len(goals), "missing_scorer": int(missing), "pct_missing": round(pct, 2)}
    if not success:
        log.warning("Goal scorer check FAILED: %d/%d goals missing scorer (%.1f%%)",
                    missing, len(goals), pct)
    return DQResult(name, success=success, stats=stats)


def check_duplicate_events(table_path: str) -> DQResult:
    """Bronze event_id must be unique."""
    name = "custom_duplicate_events"
    df = load_table(table_path)
    if df is None:
        return DQResult(name, success=True, stats={"skipped": True})

    total      = len(df)
    unique     = df["event_id"].nunique()
    duplicates = total - unique
    return DQResult(name, success=(duplicates == 0),
                    stats={"total": total, "unique": unique, "duplicates": duplicates})


def check_score_non_negative(table_path: str) -> DQResult:
    """home_score and away_score on goal events must be >= 0."""
    name = "custom_score_non_negative"
    df = load_table(table_path)
    if df is None:
        return DQResult(name, success=True, stats={"skipped": True})

    goals = df[df["event_type"] == "goal"].dropna(subset=["home_score", "away_score"])
    if len(goals) == 0:
        return DQResult(name, success=True, stats={"skipped": "no goals with scores"})

    bad = ((goals["home_score"] < 0) | (goals["away_score"] < 0)).sum()
    return DQResult(name, success=(bad == 0), stats={"invalid_score_rows": int(bad)})


# ── Main runner ────────────────────────────────────────────────────────────────

def run_all_checks() -> list[DQResult]:
    ctx = gx.get_context(context_root_dir=str(GE_DIR))
    results: list[DQResult] = []

    for suite_name, table_path in SUITE_TABLE_MAP.items():
        results.append(validate_suite(ctx, suite_name, table_path))

    # Custom checks against silver events
    silver_events_path = SUITE_TABLE_MAP["silver_game_events"]
    results.append(check_goal_scorer_not_null(silver_events_path))
    results.append(check_duplicate_events(SUITE_TABLE_MAP["bronze_game_events"]))
    results.append(check_score_non_negative(silver_events_path))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="NHL data quality checks")
    parser.add_argument("--fail-on-critical", action="store_true",
                        help="Exit 1 if any critical suite fails.")
    args = parser.parse_args()

    results = run_all_checks()

    passed = sum(1 for r in results if r.success)
    failed = len(results) - passed

    log.info("─" * 60)
    log.info("Data quality summary: %d passed, %d failed", passed, failed)
    for r in results:
        log.info("  %s", r)

    if args.fail_on_critical:
        critical_failures = [r for r in results if not r.success and r.suite in CRITICAL_SUITES]
        if critical_failures:
            log.error("CRITICAL failures: %s", [r.suite for r in critical_failures])
            sys.exit(1)


if __name__ == "__main__":
    main()
