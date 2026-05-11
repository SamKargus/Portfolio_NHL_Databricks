import logging
from datetime import datetime
from pathlib import Path

import great_expectations as ge

# ----------------------------------------------------------------------
# Setup (run once)
# ----------------------------------------------------------------------
_cwd = Path.cwd()
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"ge_bronze_year_coverage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("ge_bronze_year_coverage")
logger.info(f"ge_bronze_year_coverage started — log: {log_file}")

# ----------------------------------------------------------------------
# Known gaps — years with no NHL games (lockouts, WW2, pre-NHL)
# These are expected failures and are not counted against the suite.
# ----------------------------------------------------------------------
KNOWN_GAPS = {
    1916,        # NHL not yet founded (founded Nov 1917)
    1919,        # 1919 Stanley Cup cancelled (Spanish flu)
    2004,        # Full lockout — entire 2004-05 season cancelled
}

START_YEAR = 1917
END_YEAR   = datetime.now().year

# ----------------------------------------------------------------------
# Run expectations year by year
# ----------------------------------------------------------------------
failures: list[int] = []
skipped:  list[int] = []
passed  = 0

logger.info(f"Checking bronze coverage for years {START_YEAR}–{END_YEAR}")

for year in range(START_YEAR, END_YEAR + 1):
    if year in KNOWN_GAPS:
        logger.info(f"SKIP  {year}: known gap (no NHL games expected)")
        skipped.append(year)
        continue

    df = spark.sql(f"""
        SELECT *
        FROM nhl.bronze.nhl_events
        WHERE raw_json LIKE '%gameDate": "{year}%'
    """)

    ge_df = ge.dataset.SparkDFDataset(df)

    result = ge_df.expect_table_row_count_to_be_between(min_value=1)

    row_count = result.result.get("observed_value", 0)

    if result.success:
        logger.info(f"PASS  {year}: {row_count:,} rows")
        passed += 1
    else:
        logger.warning(f"FAIL  {year}: 0 rows — no bronze events found")
        failures.append(year)

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
total_checked = passed + len(failures)
logger.info("=" * 60)
logger.info(
    f"Results: {passed}/{total_checked} passed | "
    f"{len(failures)} failed | {len(skipped)} skipped (known gaps)"
)

if failures:
    logger.warning(f"Missing years: {failures}")
    raise AssertionError(
        f"Bronze year coverage check FAILED — {len(failures)} year(s) missing data: {failures}"
    )
else:
    logger.info("All expected years have bronze coverage.")
