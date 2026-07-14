# NHL Play-by-Play Data Pipeline

A batch data engineering pipeline that ingests every NHL play-by-play game from the NHL public API into a Databricks Lakehouse using the **Medallion Architecture** (Bronze → Silver → Gold). The pipeline covers the full history of NHL games from the 1917-18 season to the present day.

---

## Architecture

```
NHL Public API
      │
      ▼
┌─────────────────────────────────────┐
│  BRONZE  nhl.bronze.nhl_events      │  Raw JSON, schema-on-read
│  1 row per game                     │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  SILVER  nhl.silver.nhl_plays       │  Parsed, typed, denormalised
│  1 row per play event               │
└─────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────┐
│  GOLD  nhl.gold.*                   │  Kimball star schema
│  fact_event + conformed dimensions  │
└─────────────────────────────────────┘
```

All layers are **Delta tables** stored in a Unity Catalog (`nhl`). Gold is built directly from **bronze**, not silver — see [Gold](#gold--batch_goldpy) for why.

---

## Project Structure

```
├── src/
│   └── batch/
│       ├── batch_bronze.py   # Ingests raw JSON from the NHL API → bronze
│       ├── batch_silver.py   # Parses and flattens bronze JSON → silver
│       └── batch_gold.py     # Models bronze JSON into a star schema → gold
├── SQL/
│   └── initTables/           # DDL notebooks for manual table setup
├── great_expectations/       # Data quality framework (in progress)
├── logs/                     # Timestamped run logs
└── config/                   # Reserved for environment config
```

---

## Pipeline Detail

### Bronze — `batch_bronze.py`

Fetches the complete play-by-play JSON payload from `https://api-web.nhle.com/v1` and writes it verbatim to the bronze Delta table. No parsing, no schema enforcement — raw vault.

**How game IDs are collected**

The NHL schedule endpoint (`/schedule/{date}`) returns a full `gameWeek` (7 days of games) per call. The script steps through the target date range **weekly** rather than daily to avoid fetching each game ID ~7 times.

```
get_all_game_ids(start, end)
  └── get_game_ids_for_day(week_start)   → up to 7 days of game IDs per call
       └── /v1/schedule/{date}
```

Results are deduplicated with `dict.fromkeys()` before any processing begins.

**Idempotency**

Before the processing loop, the script queries `DISTINCT source` from the bronze table and skips any game whose URL is already present.

**Rate limiting**

Requests to the schedule endpoint use exponential backoff (up to 5 retries, `2^attempt` second wait) on HTTP 429 responses.

**Bronze table schema**

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | BIGINT IDENTITY | Surrogate key, auto-generated |
| `ingestion_timestamp` | TIMESTAMP | Time the row was written |
| `source` | STRING | Full API URL used to fetch the game |
| `raw_json` | STRING | Complete JSON response from the API |

---

### Silver — `batch_silver.py`

Reads every unprocessed row from bronze, parses the raw JSON, and writes one flat row per play event to the silver table. Game-level context (venue, teams, final score) is **denormalised onto every play row** so downstream queries never need to join back to a game dimension.

**Field contract validation**

The script maintains explicit sets of known and required fields at every level of the JSON (game, team, play, play details). On each run:

- **New unmapped field detected** → `WARNING` log (fires once per field name per run). The field exists in bronze but is not yet captured in silver.
- **Required mapped field missing** → `ERROR` log + `ValueError` raised. The pipeline halts immediately. This indicates an API breaking change (field renamed or removed).

This means bronze always acts as a full safety net — no data is ever lost, and silver failures are explicit rather than silent nulls.

**Idempotency**

Queries `DISTINCT game_id` from silver on startup and skips any already-processed games.

**Chunked writes**

Rows are accumulated in memory and flushed to Delta every 500 games (`CHUNK_SIZE`) to avoid OOM on large historical backfills.

**Schema evolution**

New columns are added to the live table via `ALTER TABLE` statements that run on every startup and silently no-op if the column already exists. This means re-running the script after adding a new field is always safe.

**Silver table schema**

*One row per play event. 61 columns.*

| Group | Columns |
|-------|---------|
| **Game identity** | `game_id`, `season`, `game_type`, `game_date`, `game_state`, `game_schedule_state`, `limited_scoring`, `special_event` |
| **Venue** | `venue`, `venue_location`, `start_time_utc`, `eastern_utc_offset`, `venue_utc_offset` |
| **Away team** | `away_team_id`, `away_team_abbrev`, `away_team_name`, `away_final_score`, `away_final_sog` |
| **Home team** | `home_team_id`, `home_team_abbrev`, `home_team_name`, `home_final_score`, `home_final_sog` |
| **Game outcome** | `last_period_type`, `reg_periods` |
| **Play identity** | `event_id`, `type_code`, `type_desc_key`, `sort_order`, `situation_code` |
| **Period & time** | `period_number`, `period_type`, `time_in_period`, `time_remaining` |
| **Coordinates** | `x_coord`, `y_coord` |
| **Shot events** | `shot_type`, `shooting_player_id`, `goalie_in_net_id`, `event_owner_team_id` |
| **Goal events** | `scoring_player_id`, `scoring_player_total`, `assist1_player_id`, `assist1_player_total`, `assist2_player_id`, `assist2_player_total`, `assist3_player_id`, `assist3_player_total` |
| **Live score** | `away_score`, `home_score`, `away_sog`, `home_sog` |
| **Hit events** | `hitting_player_id`, `hittee_player_id` |
| **Block events** | `blocking_player_id` |
| **Faceoff events** | `winning_player_id`, `losing_player_id` |
| **Penalty events** | `penalty_type_code`, `penalty_desc_key`, `penalty_duration`, `committed_by_player_id`, `drawn_by_player_id`, `served_by_player_id` |
| **Stoppage events** | `stoppage_reason` |
| **Generic player** | `player_id` (takeaways, giveaways) |
| **Metadata** | `ingestion_timestamp` |

---

### Gold — `batch_gold.py`

A **Kimball star schema** — a single atomic fact table at *play* grain surrounded by conformed dimensions. All tables are fully overwritten each run (idempotent, safe to re-run) and use **natural business keys** (real `player_id` / `team_id` / `game_id` / `yyyymmdd` `date_key`) as join keys, which are friendlier for ad-hoc SQL than opaque surrogates and collision-free.

**Built from bronze, not silver.** Silver is deliberately left un-modelled: it is flat, denormalised, and carries player *names* but no player *ids*. Gold re-parses the bronze JSON (`from_json` + `explode`, fully distributed — no driver-side work) because bronze is the only place that still holds real NHL `player_id`s (in each play's `details` and in `rosterSpots`). Linking facts to players by name would reintroduce the duplicate-name collisions (e.g. two "Sebastian Aho") the id-based model exists to avoid.

```
        dim_date ─┐                 ┌─ dim_team
                  ├──  fact_event  ─┤
        dim_game ─┘                 ├─ dim_player  (role-playing)
   dim_event_type ─────────────────┘
```

| Table | Grain | Notes |
|-------|-------|-------|
| `fact_event` | one row per play `(game_id, event_id)` | Role-playing `player_id` FKs per play role (scorer, assists 1–3, shooter, goalie, hitter, hittee, blocker, faceoff win/lose, penalty committed/drawn/served, generic). `event_owner_team_id` gives team-at-time-of-event. |
| `dim_player` | one row per `player_id` | **Type 1** — current descriptive attributes only. |
| `dim_player_team` | one row per contiguous `(player_id, team_id)` spell | Player↔team **stint history** — the SCD for trades, with `valid_from` / `valid_to` / `is_current` / `games_in_stint`, derived from per-game `rosterSpots` via gaps-and-islands. Kept off `dim_player` so `player_id` stays a clean unique FK on the fact. |
| `dim_team` | one row per `team_id` | Most recent team attributes (abbrev, name, logos). |
| `dim_game` | one row per `game_id` | Game-level context (season, `game_type`, venue, final score/SOG, outcome). |
| `dim_date` | one row per calendar date | Standard calendar dimension keyed by `yyyymmdd` `date_key`. |
| `dim_event_type` | one row per `type_code` | Event-type lookup, derived from the fact. |

Primary consumer is **ad-hoc SQL / notebooks**. Older non-dimensional gold tables (`player_stats`, `team_stats`, `dim_players`) are dropped by this script as superseded.

---

## Setup

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- A catalog named `nhl` (or update the three-part table names in both scripts)
- Python packages: `requests`, `pyspark` (both available in the Databricks runtime)

### Running the pipeline

Run each script as a Databricks notebook or job. `spark` is assumed to be available in the session context.

**Step 1 — Bronze ingestion**

```python
# In batch_bronze.py, set your date range:
START_DATE = date(1917, 12, 1)
END_DATE   = date.today()
```

Then run `batch_bronze.py`. On the first run this will take several hours to backfill the full history. Subsequent runs only process new games.

**Step 2 — Silver transformation**

Run `batch_silver.py` after bronze completes. No configuration needed — it automatically detects and processes all bronze rows not yet in silver.

**Step 3 — Gold modelling**

Run `batch_gold.py` after bronze completes (it reads bronze, so it does not depend on silver). No configuration needed — it fully rebuilds the star schema each run.

All three scripts are safe to re-run at any time.

---

## Data Coverage

| Season range | Notes |
|---|---|
| 1917–present | Full history available via the NHL API |
| Pre-~2000 | Some fields absent (e.g. `sog`, coordinates) — stored as `NULL` |
| Pre-~1930 | Up to 3 assists per goal recorded (`assist3_player_id`) |

---

## Logging

Both scripts write timestamped logs to the `logs/` directory and to stdout simultaneously.

```
logs/
├── batch_bronze_20260508_004500.log
└── batch_silver_20260508_004631.log
```

Key log events:

| Level | Meaning |
|-------|---------|
| `INFO` | Normal progress — games found, chunks flushed, run summary |
| `WARNING` | New unmapped API field detected — safe to continue, but schema update recommended |
| `ERROR` | Transient error on a single row (game retried on next run) or pipeline halt |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Compute | Databricks (Spark Connect) |
| Storage | Delta Lake (Unity Catalog) |
| Source API | NHL Public API (`api-web.nhle.com/v1`) |
| Language | Python 3.12 |
| Data quality | Great Expectations (in progress) |
