"""
app.py — NHL Playoff Analytics Live Dashboard
Run with: streamlit run dashboard/app.py
Live scoreboards auto-refresh via st.fragment every 10 s.
Historical game data is cached aggressively (1 hour).
"""

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NHL Playoff Analytics",
    page_icon="🏒",
    layout="wide",
    initial_sidebar_state="expanded",
)

_DATABRICKS_HOST      = os.environ["DATABRICKS_HOST"]          # e.g. adb-xxx.azuredatabricks.net
_DATABRICKS_TOKEN     = os.environ["DATABRICKS_TOKEN"]
_DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_SQL_HTTP_PATH"]  # /sql/1.0/warehouses/xxx
_DATABRICKS_CATALOG   = os.getenv("DATABRICKS_CATALOG", "hive_metastore")
_DATABRICKS_SCHEMA    = os.getenv("DATABRICKS_SCHEMA",  "nhl")

DB_URL = (
    f"databricks://token:{_DATABRICKS_TOKEN}@{_DATABRICKS_HOST}"
    f"?http_path={_DATABRICKS_HTTP_PATH}"
    f"&catalog={_DATABRICKS_CATALOG}&schema={_DATABRICKS_SCHEMA}"
)

TABLE_NAMES = [
    "bronze_game_events", "silver_game_events", "silver_player_stats",
    "gold_player_scoring", "gold_team_momentum", "gold_penalty_summary",
]


# ── DB engine — one engine per Streamlit server process ───────────────────────
@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def _query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=300)
def table_row_count(table: str) -> int:
    try:
        df = _query(f"SELECT COUNT(*) AS cnt FROM {table}")
        return int(df["cnt"].iloc[0])
    except Exception:
        return -1


# ── Game index — lightweight, long TTL (historical games never change) ─────────
@st.cache_data(ttl=300)
def load_game_index() -> pd.DataFrame:
    """One row per game: game_id, season, home_team, away_team, final score."""
    try:
        return _query("""
            SELECT game_id, season, game_type,
                   home_team_abbrev, away_team_abbrev,
                   home_score, away_score, period_type
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_second DESC) AS _rn
                FROM silver_game_events
                WHERE home_score IS NOT NULL
            ) t
            WHERE _rn = 1
        """)
    except Exception:
        return pd.DataFrame()


# ── Live scores — small table, refreshed fast inside fragment ──────────────────
@st.cache_data(ttl=5)
def load_game_scores() -> pd.DataFrame:
    """Only return games updated in the last 15 minutes — anything older has ended."""
    try:
        return _query(
            "SELECT * FROM game_scores WHERE updated_at > current_timestamp() - INTERVAL 15 MINUTES ORDER BY updated_at DESC"
        )
    except Exception:
        return pd.DataFrame()


# ── Player lookup — rarely changes ────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_players() -> dict:
    try:
        df = _query("SELECT player_id, full_name FROM players")
        return dict(zip(df["player_id"], df["full_name"]))
    except Exception:
        return {}


# ── Per-game loaders — long TTL for historical, short TTL for live ─────────────
# We use separate cached functions so Streamlit can invalidate them independently.

@st.cache_data(ttl=3600)
def load_events_historical(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM silver_game_events WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_events_live(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM silver_game_events WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_stats_historical(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM silver_player_stats WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_stats_live(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM silver_player_stats WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_scoring_historical(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM gold_player_scoring WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_scoring_live(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM gold_player_scoring WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_momentum_historical(game_id: int) -> pd.DataFrame:
    try:
        return _query(
            "SELECT * FROM gold_team_momentum WHERE game_id = :gid ORDER BY window_start",
            {"gid": game_id},
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_momentum_live(game_id: int) -> pd.DataFrame:
    try:
        return _query(
            "SELECT * FROM gold_team_momentum WHERE game_id = :gid ORDER BY window_start",
            {"gid": game_id},
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_penalties_historical(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM gold_penalty_summary WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_penalties_live(game_id: int) -> pd.DataFrame:
    try:
        return _query("SELECT * FROM gold_penalty_summary WHERE game_id = :gid", {"gid": game_id})
    except Exception:
        return pd.DataFrame()


# ── Season-level loaders (for standings / leaderboards) ───────────────────────
@st.cache_data(ttl=600)
def load_season_team_stats(season: int | None) -> pd.DataFrame:
    """Per-team W/L/OTL/Pts/GF/GA/Diff from game outcomes.
    NHL points: 2 for a win, 1 for OT/SO loss, 0 for regulation loss."""
    where = "WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    params: dict = {}
    if season is not None:
        where += " AND season = :season"
        params["season"] = int(season)
    try:
        return _query(
            f"""
            WITH finals AS (
                SELECT game_id, season, game_type,
                       home_team_abbrev, away_team_abbrev,
                       home_score, away_score, period_type
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_second DESC) AS _rn
                    FROM silver_game_events
                    {where}
                ) t
                WHERE _rn = 1
            ),
            per_team AS (
                SELECT home_team_abbrev AS team, home_score AS gf, away_score AS ga,
                       (home_score > away_score) AS win,
                       (home_score < away_score) AS loss,
                       period_type
                FROM finals
                UNION ALL
                SELECT away_team_abbrev AS team, away_score AS gf, home_score AS ga,
                       (away_score > home_score) AS win,
                       (away_score < home_score) AS loss,
                       period_type
                FROM finals
            )
            SELECT
                team,
                COUNT(*)                                     AS gp,
                SUM(CASE WHEN win THEN 1 ELSE 0 END)         AS wins,
                SUM(CASE WHEN loss AND period_type = 'REG' THEN 1 ELSE 0 END) AS reg_losses,
                SUM(CASE WHEN loss AND period_type IN ('OT','SO') THEN 1 ELSE 0 END) AS ot_losses,
                SUM(gf)                                      AS gf,
                SUM(ga)                                      AS ga,
                SUM(gf) - SUM(ga)                            AS diff,
                SUM(CASE WHEN win THEN 2 ELSE 0 END) +
                SUM(CASE WHEN loss AND period_type IN ('OT','SO') THEN 1 ELSE 0 END) AS pts
            FROM per_team
            WHERE team IS NOT NULL
            GROUP BY team
            ORDER BY pts DESC, wins DESC, diff DESC
            """,
            params,
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_season_player_leaders(season: int | None, limit: int = 50) -> pd.DataFrame:
    """Top scorers across games — aggregates silver_player_stats."""
    where = "WHERE 1=1"
    params: dict = {"lim": int(limit)}
    if season is not None:
        where += " AND season = :season"
        params["season"] = int(season)
    try:
        return _query(
            f"""
            SELECT
                player_id,
                MAX(player_name)  AS player_name,
                MAX(team_abbrev)  AS team_abbrev,
                MAX(position)     AS position,
                COUNT(DISTINCT game_id) AS gp,
                SUM(goals)        AS goals,
                SUM(assists)      AS assists,
                SUM(points)       AS points,
                SUM(shots)        AS shots,
                SUM(hits)         AS hits,
                SUM(blocked_shots) AS blocks,
                SUM(pim)          AS pim,
                SUM(plus_minus)   AS plus_minus,
                SUM(toi_seconds)  AS toi_seconds
            FROM silver_player_stats
            {where}
            GROUP BY player_id
            HAVING SUM(points) > 0 OR SUM(shots) > 0
            ORDER BY points DESC, goals DESC
            LIMIT :lim
            """,
            params,
        )
    except Exception:
        return pd.DataFrame()


# ── Score & shots by period (used in scoreboard panel) ────────────────────────
@st.cache_data(ttl=3600)
def load_period_breakdown_historical(game_id: int) -> pd.DataFrame:
    return _period_breakdown(game_id)


@st.cache_data(ttl=5)
def load_period_breakdown_live(game_id: int) -> pd.DataFrame:
    return _period_breakdown(game_id)


def _period_breakdown(game_id: int) -> pd.DataFrame:
    """Goals and shots-on-goal per period, per team.
    Shooter team = opposite of goalie_in_net_id's team (goalie is on defending team)."""
    try:
        return _query(
            """
            WITH attributed AS (
                SELECT
                    e.period,
                    e.period_type,
                    e.event_type,
                    CASE
                        WHEN g.team_abbrev = e.home_team_abbrev THEN e.away_team_abbrev
                        WHEN g.team_abbrev = e.away_team_abbrev THEN e.home_team_abbrev
                        ELSE NULL
                    END AS team_abbrev
                FROM silver_game_events e
                LEFT JOIN silver_player_stats g
                       ON g.game_id = e.game_id AND g.player_id = e.goalie_in_net_id
                WHERE e.game_id = :gid
                  AND e.event_type IN ('goal', 'shot-on-goal')
                  AND e.period IS NOT NULL
            )
            SELECT period, period_type, team_abbrev, event_type, COUNT(*) AS cnt
            FROM attributed
            GROUP BY period, period_type, team_abbrev, event_type
            ORDER BY period
            """,
            {"gid": game_id},
        )
    except Exception:
        return pd.DataFrame()


# ── Live snapshot — queries bronze (silver lags for in-progress games) ────────
@st.cache_data(ttl=5)
def load_live_snapshot(game_id: int) -> pd.DataFrame:
    """Bronze events joined to most-recent-team lookup, so we can attribute
    shots (via goalie) and penalties (via committed-by) even when silver
    player_stats doesn't yet have rows for this game."""
    try:
        return _query(
            """
            WITH player_last_team AS (
                SELECT player_id, team_abbrev
                FROM (
                    SELECT player_id, team_abbrev,
                           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_id DESC) AS _rn
                    FROM silver_player_stats
                    WHERE team_abbrev IS NOT NULL
                ) t
                WHERE _rn = 1
            )
            SELECT
                e.event_id, e.event_type, e.period, e.period_type, e.time_in_period,
                e.home_team_abbrev, e.away_team_abbrev,
                e.penalty_type, e.duration_minutes,
                CASE
                    WHEN plt_g.team_abbrev IN (e.home_team_abbrev, e.away_team_abbrev)
                    THEN plt_g.team_abbrev ELSE NULL
                END AS goalie_team,
                CASE
                    WHEN plt_p.team_abbrev IN (e.home_team_abbrev, e.away_team_abbrev)
                    THEN plt_p.team_abbrev ELSE NULL
                END AS penalty_player_team,
                (e.period - 1) * 1200
                  + CAST(SPLIT_PART(e.time_in_period, ':', 1) AS INT) * 60
                  + CAST(SPLIT_PART(e.time_in_period, ':', 2) AS INT)
                  AS game_second
            FROM bronze_game_events e
            LEFT JOIN player_last_team plt_g ON plt_g.player_id = e.goalie_in_net_id
            LEFT JOIN player_last_team plt_p ON plt_p.player_id = e.committed_by_player_id
            WHERE e.game_id = :gid
              AND e.time_in_period IS NOT NULL
            """,
            {"gid": game_id},
        )
    except Exception:
        return pd.DataFrame()


# ── Shot-attempt / Corsi loader ───────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_shot_attempts_historical(game_id: int) -> pd.DataFrame:
    return _shot_attempts(game_id)


@st.cache_data(ttl=5)
def load_shot_attempts_live(game_id: int) -> pd.DataFrame:
    return _shot_attempts(game_id)


def _shot_attempts(game_id: int) -> pd.DataFrame:
    """All shot-attempt events with shooter team attribution for Corsi/Fenwick.
    Shooter team is derived from goalie_in_net_id (opposite team). Blocked shots
    have no goalie attribution, so their shooter_team is NULL."""
    try:
        return _query(
            """
            SELECT
                e.event_id, e.event_type, e.period, e.game_second,
                e.x_coord, e.y_coord, e.zone_code, e.shot_type,
                e.home_team_abbrev, e.away_team_abbrev,
                CASE
                    WHEN g.team_abbrev = e.home_team_abbrev THEN e.away_team_abbrev
                    WHEN g.team_abbrev = e.away_team_abbrev THEN e.home_team_abbrev
                    ELSE NULL
                END AS shooter_team
            FROM silver_game_events e
            LEFT JOIN silver_player_stats g
                   ON g.game_id = e.game_id AND g.player_id = e.goalie_in_net_id
            WHERE e.game_id = :gid
              AND e.event_type IN ('goal', 'shot-on-goal', 'missed-shot', 'blocked-shot')
            """,
            {"gid": game_id},
        )
    except Exception:
        return pd.DataFrame()


def load_game_data(game_id: int, is_live: bool):
    """Route to the right TTL variant based on whether game is live."""
    if is_live:
        return (
            load_events_live(game_id),
            load_stats_live(game_id),
            load_scoring_live(game_id),
            load_momentum_live(game_id),
            load_penalties_live(game_id),
        )
    return (
        load_events_historical(game_id),
        load_stats_historical(game_id),
        load_scoring_historical(game_id),
        load_momentum_historical(game_id),
        load_penalties_historical(game_id),
    )


def resolve(player_id, lookup: dict) -> str:
    if pd.isna(player_id):
        return ""
    return lookup.get(int(player_id), str(int(player_id)))


def season_label(s) -> str:
    s = int(s)
    y = s // 10000 if s > 9999 else s
    return f"{y}-{str(y + 1)[2:]}"


def fmt_toi(seconds) -> str:
    if pd.isna(seconds) or seconds is None:
        return ""
    total = int(seconds)
    return f"{total // 60}:{total % 60:02d}"


def load_period_breakdown(game_id: int, is_live: bool) -> pd.DataFrame:
    return load_period_breakdown_live(game_id) if is_live else load_period_breakdown_historical(game_id)


def load_shot_attempts(game_id: int, is_live: bool) -> pd.DataFrame:
    return load_shot_attempts_live(game_id) if is_live else load_shot_attempts_historical(game_id)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏒 NHL Playoff Analytics")
    st.caption("Live data pipeline — Databricks Delta backend")

    auto_refresh = st.toggle("Auto-refresh (live games only)", value=True)
    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**Pipeline layers**")
    for t in TABLE_NAMES:
        count = table_row_count(t)
        icon  = "🟢" if count > 0 else ("🔴" if count == 0 else "⚠️")
        label = t.replace("_", " ")
        st.markdown(f"{icon} `{label}` — {count:,} rows" if count >= 0 else f"⚠️ `{label}` — error")

    st.divider()
    st.caption(f"Last loaded: {pd.Timestamp.now().strftime('%H:%M:%S')}")


# ── Load index & live scores (fast path) ──────────────────────────────────────
st.title("🏒 NHL Playoff Analytics — Live Dashboard")

players = load_players()
index   = load_game_index()

if index.empty:
    st.warning("No data yet — waiting for pipeline to load data.")
    st.stop()

# live_ids comes from game_scores (tiny table, fast)
scores   = load_game_scores()
live_ids = set(scores["game_id"].tolist()) if not scores.empty else set()

# Merge any live games that aren't yet in the historical index (e.g. just started,
# silver hasn't caught up, or score is still NULL in events).
if not scores.empty:
    missing = scores[~scores["game_id"].isin(index["game_id"])].copy()
    if not missing.empty:
        # NHL game_id format: YYYYTTGGGG (season | type | game num)
        missing["season"]     = (missing["game_id"] // 1000000).astype(int)
        missing["game_type"]  = ((missing["game_id"] // 10000) % 100).astype(int)
        missing = missing.rename(columns={"period_type": "period_type"})[[
            "game_id", "season", "game_type",
            "home_team_abbrev", "away_team_abbrev",
            "home_score", "away_score", "period_type",
        ]]
        index = pd.concat([index, missing], ignore_index=True)

# ── Pipeline health ────────────────────────────────────────────────────────────
st.subheader("Pipeline Health")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Bronze Events",  f"{table_row_count('bronze_game_events'):,}")
c2.metric("Silver Events",  f"{table_row_count('silver_game_events'):,}")
c3.metric("Silver Players", f"{table_row_count('silver_player_stats'):,}")
c4.metric("Gold Scoring",   f"{table_row_count('gold_player_scoring'):,} players")
c5.metric("Gold Penalties", f"{table_row_count('gold_penalty_summary'):,} records")

st.divider()


# ── Filters ────────────────────────────────────────────────────────────────────
st.subheader("Find a Game")
f1, f2, f3 = st.columns([1, 1, 1])

all_seasons = sorted(index["season"].dropna().unique().tolist(), reverse=True)
LIVE_ONLY_LABEL = f"🔴 Live Now ({len(live_ids)})"
season_options = [LIVE_ONLY_LABEL, "All seasons"] + [season_label(s) for s in all_seasons]
# Default to Live Now when any games are live, otherwise All seasons
default_season_idx = 0 if live_ids else 1
selected_season_label = f1.selectbox("Season", season_options, index=default_season_idx)

if selected_season_label == LIVE_ONLY_LABEL:
    filtered_index = index[index["game_id"].isin(live_ids)].copy()
elif selected_season_label == "All seasons":
    filtered_index = index.copy()
else:
    matched = [s for s in all_seasons if season_label(s) == selected_season_label]
    filtered_index = index[index["season"].isin(matched)] if matched else index.copy()

game_type_map = {1: "Preseason", 2: "Regular Season", 3: "Playoffs"}
all_types = sorted(filtered_index["game_type"].dropna().unique().tolist())
type_options = ["All"] + [game_type_map.get(int(t), str(t)) for t in all_types]
selected_type_label = f2.selectbox("Game Type", type_options)

if selected_type_label != "All":
    type_code = {v: k for k, v in game_type_map.items()}.get(selected_type_label)
    if type_code:
        filtered_index = filtered_index[filtered_index["game_type"] == type_code]

all_teams = sorted(set(
    filtered_index["home_team_abbrev"].dropna().tolist() +
    filtered_index["away_team_abbrev"].dropna().tolist()
))
selected_teams = f3.multiselect("Teams (pick one or more)", all_teams)

if selected_teams:
    mask = (
        filtered_index["home_team_abbrev"].isin(selected_teams) |
        filtered_index["away_team_abbrev"].isin(selected_teams)
    )
    if len(selected_teams) == 2:
        t1, t2 = selected_teams
        both_mask = (
            (filtered_index["home_team_abbrev"] == t1) & (filtered_index["away_team_abbrev"] == t2)
        ) | (
            (filtered_index["home_team_abbrev"] == t2) & (filtered_index["away_team_abbrev"] == t1)
        )
        show_only_matchup = st.checkbox(f"Only show {t1} vs {t2} head-to-head", value=False)
        filtered_index = filtered_index[both_mask if show_only_matchup else mask]
    else:
        filtered_index = filtered_index[mask]

if filtered_index.empty:
    st.info("No games match the selected filters.")
    st.stop()


# ── Game selector ──────────────────────────────────────────────────────────────
live_in_filter  = [g for g in sorted(live_ids, reverse=True) if g in filtered_index["game_id"].values]
other_in_filter = (
    filtered_index[~filtered_index["game_id"].isin(live_ids)]
    .sort_values("game_id", ascending=False)["game_id"]
    .tolist()
)
game_ids_ordered = live_in_filter + other_in_filter


def _game_label(gid):
    row = filtered_index[filtered_index["game_id"] == gid]
    if row.empty:
        return f"Game {gid}"
    r    = row.iloc[0]
    home = r.get("home_team_abbrev", "?")
    away = r.get("away_team_abbrev", "?")
    hs   = int(r.get("home_score") or 0)
    as_  = int(r.get("away_score") or 0)
    szn  = season_label(r["season"]) if pd.notna(r.get("season")) else ""
    live_tag = "🔴 LIVE — " if gid in live_ids else ""
    return f"{live_tag}{home} {hs}–{as_} {away}  ({szn})  [ID: {gid}]"


selected_game    = st.selectbox("Select Game", options=game_ids_ordered, format_func=_game_label)
selected_is_live = selected_game in live_ids

st.divider()


# ── Scoreboard ─────────────────────────────────────────────────────────────────
# Live scoreboard: wrapped in a fragment that auto-refreshes every 5 s
# independently — no full page rerun needed.


def _parse_clock_secs(clock: str) -> int:
    """Parse 'MM:SS' into seconds; return 0 on failure."""
    if not isinstance(clock, str) or ":" not in clock:
        return 0
    try:
        m, s = clock.split(":")
        return int(m) * 60 + int(s)
    except (ValueError, TypeError):
        return 0


def _current_game_second(period: int, time_remaining: str, period_type: str, in_intermission: bool) -> int:
    """Compute current game clock second from live game_scores row."""
    period_len = 1200  # 20 min in regulation + playoff OT
    if period_type == "OT" and period > 3:
        # Regular-season OT is 5 min; playoff OT is 20 min. Default to 20.
        period_len = 1200
    if in_intermission:
        return period * period_len  # end of just-finished period
    elapsed = period_len - _parse_clock_secs(time_remaining or "")
    return (period - 1) * period_len + max(elapsed, 0)


def _active_penalties(snap: pd.DataFrame, now_gs: int) -> pd.DataFrame:
    """Filter penalty events that are still ticking at `now_gs`.
    Skips majors/match penalties where duration is unknown, and ignores the
    'minor-ends-on-goal' rule (shown as approximate)."""
    if snap.empty:
        return pd.DataFrame()
    pens = snap[snap["event_type"] == "penalty"].copy()
    if pens.empty:
        return pens
    pens["duration_minutes"] = pd.to_numeric(pens["duration_minutes"], errors="coerce").fillna(0).astype(int)
    pens = pens[pens["duration_minutes"] > 0]
    pens["end_gs"]      = pens["game_second"] + pens["duration_minutes"] * 60
    pens["remain_secs"] = (pens["end_gs"] - now_gs).clip(lower=0)
    return pens[pens["end_gs"] > now_gs].sort_values("end_gs")


@st.fragment(run_every=5 if selected_is_live else None)
def render_scoreboard(game_id: int, is_live: bool):
    st.subheader(f"📊 Scoreboard — {_game_label(game_id)}")

    if is_live:
        live_scores = load_game_scores()
        game_score_row = live_scores[live_scores["game_id"] == game_id] if not live_scores.empty else pd.DataFrame()
    else:
        game_score_row = pd.DataFrame()

    if not game_score_row.empty:
        gs = game_score_row.iloc[0]
        home    = gs.get("home_team_abbrev", "HOME")
        away    = gs.get("away_team_abbrev", "AWAY")
        h_score = int(gs.get("home_score", 0) or 0)
        a_score = int(gs.get("away_score", 0) or 0)
        period  = int(gs.get("period", 1) or 1)
        period_type     = gs.get("period_type", "")
        time_rem        = gs.get("time_remaining", "")
        in_intermission = bool(gs.get("in_intermission", False))

        if in_intermission:
            p_suffix     = {1: "st", 2: "nd", 3: "rd"}.get(period, "th")
            period_label  = "Intermission"
            clock_display = f"End of {period}{p_suffix}"
        elif period_type == "OT":
            period_label  = "OT"
            clock_display = time_rem
        elif period_type == "SO":
            period_label  = "Shootout"
            clock_display = ""
        else:
            period_label  = {1: "1st", 2: "2nd", 3: "3rd"}.get(period, f"OT{period - 3}")
            clock_display = time_rem

        sc1, sc2, sc3 = st.columns([2, 1, 2])
        sc1.metric(home, h_score)
        sc2.metric(period_label, clock_display)
        sc3.metric(away, a_score)

        # Live-only panels: SOG per team + power-play status
        if is_live:
            snap = load_live_snapshot(game_id)
            if not snap.empty:
                # SOG per team (shooter = opposite of goalie's team)
                sog = snap[snap["event_type"] == "shot-on-goal"].copy()
                sog["shooter_team"] = sog.apply(
                    lambda r: r["away_team_abbrev"] if r["goalie_team"] == r["home_team_abbrev"]
                    else (r["home_team_abbrev"] if r["goalie_team"] == r["away_team_abbrev"] else None),
                    axis=1,
                )
                sog_counts = sog["shooter_team"].value_counts()
                h_sog = int(sog_counts.get(home, 0))
                a_sog = int(sog_counts.get(away, 0))

                sg1, sg2, sg3 = st.columns([2, 1, 2])
                sg1.metric(f"{home} SOG", h_sog)
                sg2.metric("Shots", "")
                sg3.metric(f"{away} SOG", a_sog)

                # Power-play status
                now_gs  = _current_game_second(period, time_rem, period_type, in_intermission)
                active  = _active_penalties(snap, now_gs)

                if active.empty:
                    st.success("⚡ Even strength (5-on-5)")
                else:
                    penalty_counts = active["penalty_player_team"].value_counts()
                    home_pen = int(penalty_counts.get(home, 0))
                    away_pen = int(penalty_counts.get(away, 0))
                    if home_pen > away_pen:
                        diff = home_pen - away_pen
                        st.warning(f"⚡ {away} on the power play (5-on-{5 - diff})")
                    elif away_pen > home_pen:
                        diff = away_pen - home_pen
                        st.warning(f"⚡ {home} on the power play (5-on-{5 - diff})")
                    else:
                        st.info("⚡ Offsetting penalties (4-on-4)")

                    # Show each active penalty with time remaining
                    disp = active[[
                        "penalty_player_team", "penalty_type",
                        "duration_minutes", "remain_secs",
                    ]].rename(columns={
                        "penalty_player_team": "Team",
                        "penalty_type": "Type",
                        "duration_minutes": "Length (min)",
                    })
                    disp["Time Left"] = disp["remain_secs"].apply(
                        lambda s: f"{int(s) // 60}:{int(s) % 60:02d}"
                    )
                    st.dataframe(
                        disp[["Team", "Type", "Length (min)", "Time Left"]],
                        use_container_width=True, hide_index=True,
                    )
                    st.caption(
                        "Power-play status is approximate: minor penalties that "
                        "end on an opposing goal are still shown until time expires."
                    )
    else:
        # Historical game — pull from index (no extra query)
        idx_row = filtered_index[filtered_index["game_id"] == game_id]
        if not idx_row.empty:
            r       = idx_row.iloc[0]
            home    = r.get("home_team_abbrev", "HOME")
            away    = r.get("away_team_abbrev", "AWAY")
            h_score = int(r.get("home_score") or 0)
            a_score = int(r.get("away_score") or 0)
            ptype   = r.get("period_type", "REG")
            flabel  = "Final/OT" if ptype == "OT" else ("Final/SO" if ptype == "SO" else "Final")
            sc1, sc2, sc3 = st.columns([2, 1, 2])
            sc1.metric(home, h_score)
            sc2.metric(flabel, "")
            sc3.metric(away, a_score)
        else:
            st.info("Score not available.")
            return

    # Score & shots by period
    pb = load_period_breakdown(game_id, is_live)
    if not pb.empty and home and away:
        goals_pb  = pb[pb["event_type"] == "goal"]
        shots_pb  = pb[pb["event_type"] == "shot-on-goal"]

        def _pivot(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame()
            return df.pivot_table(
                index="team_abbrev", columns="period", values="cnt", aggfunc="sum",
            ).fillna(0).astype(int)

        gp = _pivot(goals_pb)
        sp = _pivot(shots_pb)

        def _order_rows(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            order = [t for t in [home, away] if t in df.index]
            leftover = [t for t in df.index if t not in order]
            return df.reindex(order + leftover)

        gp = _order_rows(gp)
        sp = _order_rows(sp)

        def _labeled(df: pd.DataFrame, total_name: str) -> pd.DataFrame:
            if df.empty:
                return df
            df = df.copy()
            df.columns = [f"P{int(c)}" if int(c) <= 3 else ("OT" if int(c) == 4 else f"OT{int(c) - 3}") for c in df.columns]
            df[total_name] = df.sum(axis=1)
            return df

        col_l, col_r = st.columns(2)
        with col_l:
            st.caption("Goals by period")
            if not gp.empty:
                st.dataframe(_labeled(gp, "G"), use_container_width=True)
            else:
                st.caption("—")
        with col_r:
            st.caption("Shots on goal by period")
            if not sp.empty:
                st.dataframe(_labeled(sp, "SOG"), use_container_width=True)
            else:
                st.caption("—")


render_scoreboard(selected_game, selected_is_live)

st.divider()


# ── Load selected game data ────────────────────────────────────────────────────
game_events, stats, gold, mom, pen = load_game_data(int(selected_game), selected_is_live)

goals = game_events[game_events["event_type"] == "goal"].copy() if not game_events.empty else pd.DataFrame()


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "📈 Event Feed",
    "🥅 Goals & Scoring",
    "👥 Team Box Score",
    "🧤 Goaltending",
    "📊 Advanced",
    "⚡ Momentum",
    "🚨 Penalties",
    "👤 Player Stats",
    "📅 Standings",
])


# ── Tab 1: Event feed ──────────────────────────────────────────────────────────
with tab1:
    st.subheader("Live Event Feed")
    if game_events.empty:
        st.info("No events loaded.")
    else:
        event_type_filter = st.multiselect(
            "Filter event types",
            options=sorted(game_events["event_type"].unique().tolist()),
            default=sorted(game_events["event_type"].unique().tolist()),
        )
        filtered = game_events[game_events["event_type"].isin(event_type_filter)]
        filtered = filtered.sort_values("game_second", ascending=False)
        display_cols = [c for c in [
            "event_type", "period", "time_in_period", "game_second",
            "zone_code", "home_score", "away_score",
        ] if c in filtered.columns]
        st.dataframe(filtered[display_cols].reset_index(drop=True), use_container_width=True, height=400)

        st.subheader("Events by Type")
        counts = game_events["event_type"].value_counts().reset_index()
        counts.columns = ["Event Type", "Count"]
        st.bar_chart(counts.set_index("Event Type"))


# ── Tab 2: Goals & scoring ─────────────────────────────────────────────────────
with tab2:
    st.subheader("Goal Log")
    if goals.empty:
        st.info("No goals recorded.")
    else:
        dg = goals.copy()
        dg["scorer"]    = dg["scoring_player_id"].apply(lambda x: resolve(x, players))
        dg["assist1"]   = dg["assist1_player_id"].apply(lambda x: resolve(x, players))
        dg["assist2"]   = dg["assist2_player_id"].apply(lambda x: resolve(x, players))
        dg["goal_type"] = dg["goal_type"].fillna("Even Strength")
        goal_cols = [c for c in [
            "period", "time_in_period", "scorer", "assist1", "assist2",
            "shot_type", "goal_type", "home_score", "away_score",
        ] if c in dg.columns]
        st.dataframe(dg[goal_cols].sort_values("time_in_period").reset_index(drop=True), use_container_width=True)

    st.subheader("Player Scoring Totals")
    if gold.empty:
        st.info("Gold scoring not yet populated.")
    else:
        dg2 = gold.copy()
        dg2["player"] = dg2["player_id"].apply(lambda x: resolve(x, players))
        display = [c for c in ["player", "goals", "primary_assists", "secondary_assists", "total_assists", "points"] if c in dg2.columns]
        sort_col = "points" if "points" in display else display[0]
        st.dataframe(dg2[display].sort_values(sort_col, ascending=False).reset_index(drop=True), use_container_width=True)

    # Goal type breakdown (EV / PP / SH / EN / PS)
    if not goals.empty and "goal_type" in goals.columns:
        st.subheader("Goal Strength Breakdown")
        gt = goals["goal_type"].fillna("ev").value_counts().reset_index()
        gt.columns = ["Goal Type", "Count"]
        st.dataframe(gt, use_container_width=True, hide_index=True)

    # Shot-type distribution (all shot attempts)
    if not game_events.empty:
        shot_events = game_events[game_events["event_type"].isin(
            ["goal", "shot-on-goal", "missed-shot", "blocked-shot"]
        )]
        if not shot_events.empty and "shot_type" in shot_events.columns:
            st.subheader("Shot Types")
            st_counts = shot_events["shot_type"].dropna().value_counts().reset_index()
            st_counts.columns = ["Shot Type", "Count"]
            st.bar_chart(st_counts.set_index("Shot Type"))


# ── Tab 3: Team Box Score ──────────────────────────────────────────────────────
with tab3:
    st.subheader("Team Box Score")
    if stats.empty:
        st.info("No player stats loaded for this game.")
    else:
        num_cols = ["goals", "assists", "points", "shots", "hits", "blocked_shots",
                    "pim", "giveaways", "takeaways", "faceoff_wins", "faceoff_attempts",
                    "plus_minus", "toi_seconds"]
        have = [c for c in num_cols if c in stats.columns]
        team_stats = stats.groupby("team_abbrev")[have].sum().astype(int)
        if "faceoff_attempts" in team_stats.columns and "faceoff_wins" in team_stats.columns:
            team_stats["faceoff_pct"] = (
                team_stats["faceoff_wins"] / team_stats["faceoff_attempts"].replace(0, float("nan")) * 100
            ).round(1)
        if "shots" in team_stats.columns and "goals" in team_stats.columns:
            team_stats["shooting_pct"] = (
                team_stats["goals"] / team_stats["shots"].replace(0, float("nan")) * 100
            ).round(1)

        # Shot attempts for/against (Corsi-ish) from events
        sa = load_shot_attempts(int(selected_game), selected_is_live)
        if not sa.empty and "shooter_team" in sa.columns:
            sa_known = sa[sa["shooter_team"].notna()]
            attempts_for = sa_known.groupby("shooter_team").size()
            # Fenwick (excl. blocked shots) — more reliably attributed
            fenwick_for = sa_known[sa_known["event_type"].isin(
                ["goal", "shot-on-goal", "missed-shot"]
            )].groupby("shooter_team").size()
            team_stats["shot_attempts_for"] = attempts_for.reindex(team_stats.index).fillna(0).astype(int)
            team_stats["fenwick_for"]       = fenwick_for.reindex(team_stats.index).fillna(0).astype(int)

        # Pretty TOI
        if "toi_seconds" in team_stats.columns:
            team_stats["toi"] = team_stats["toi_seconds"].apply(fmt_toi)
            team_stats = team_stats.drop(columns=["toi_seconds"])

        display_order = [c for c in [
            "goals", "assists", "points", "shots", "shooting_pct",
            "shot_attempts_for", "fenwick_for",
            "hits", "blocked_shots", "pim", "giveaways", "takeaways",
            "faceoff_wins", "faceoff_attempts", "faceoff_pct",
            "plus_minus", "toi",
        ] if c in team_stats.columns]
        st.dataframe(team_stats[display_order], use_container_width=True)

        st.caption(
            "Shot attempts = goals + shots on goal + missed shots + blocked shots (Corsi). "
            "Fenwick excludes blocked shots. Blocked-shot attribution is approximate "
            "(game-total only when shooter team is unknown)."
        )


# ── Tab 4: Goaltending ─────────────────────────────────────────────────────────
with tab4:
    st.subheader("Goaltending")
    if stats.empty:
        st.info("No player stats loaded for this game.")
    else:
        # A row is a goalie if they faced shots (shots_against populated) or position='G'
        goalies = stats.copy()
        if "position" in goalies.columns:
            goalies = goalies[(goalies["position"] == "G") | (goalies.get("shots_against", 0) > 0)]
        else:
            goalies = goalies[goalies.get("shots_against", 0) > 0]

        if goalies.empty:
            st.info("No goalie data yet for this game.")
        else:
            goalies = goalies.copy()
            goalies["player_name"] = goalies["player_id"].apply(lambda x: resolve(x, players))

            if "save_pct" in goalies.columns:
                goalies["sv_pct"] = (goalies["save_pct"] * 100).round(2)

            # GAA = goals_against / (toi_seconds/3600) * 60  -> per 60 min
            if "goals_against" in goalies.columns and "toi_seconds" in goalies.columns:
                toi_min = goalies["toi_seconds"] / 60.0
                goalies["gaa"] = (goalies["goals_against"] * 60.0 / toi_min.replace(0, float("nan"))).round(2)

            if "toi_seconds" in goalies.columns:
                goalies["toi"] = goalies["toi_seconds"].apply(fmt_toi)

            goalies["shutout"] = ((goalies.get("goals_against", 1) == 0) &
                                   (goalies.get("toi_seconds", 0) >= 3000)).map({True: "✓", False: ""})

            display_cols = [c for c in [
                "player_name", "team_abbrev", "saves", "shots_against",
                "goals_against", "sv_pct", "gaa", "toi", "shutout",
            ] if c in goalies.columns]
            sort_col = "sv_pct" if "sv_pct" in display_cols else "saves"
            st.dataframe(
                goalies[display_cols].sort_values(sort_col, ascending=False).reset_index(drop=True),
                use_container_width=True,
            )
            st.caption("SV% = saves ÷ shots faced. GAA = goals allowed per 60 min. Shutout flag: 0 GA and ≥50 min played.")


# ── Tab 5: Advanced ────────────────────────────────────────────────────────────
with tab5:
    st.subheader("Advanced Shot Analytics")
    sa = load_shot_attempts(int(selected_game), selected_is_live)

    if sa.empty:
        st.info("No shot-attempt data available.")
    else:
        # Totals
        totals = sa["event_type"].value_counts()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Goals",         int(totals.get("goal", 0)))
        c2.metric("Shots on Goal", int(totals.get("shot-on-goal", 0)))
        c3.metric("Missed Shots",  int(totals.get("missed-shot", 0)))
        c4.metric("Blocked Shots", int(totals.get("blocked-shot", 0)))

        # Team Corsi / Fenwick (team attribution from goalie)
        known = sa[sa["shooter_team"].notna()]
        if not known.empty:
            corsi = (
                known.groupby(["shooter_team", "event_type"])
                .size()
                .unstack(fill_value=0)
            )
            corsi["CF_attempts"] = corsi.sum(axis=1)
            corsi["FF_attempts"] = corsi.drop(columns=["blocked-shot"], errors="ignore").sum(axis=1)
            total_cf = corsi["CF_attempts"].sum()
            total_ff = corsi["FF_attempts"].sum()
            if total_cf:
                corsi["CF%"] = (corsi["CF_attempts"] / total_cf * 100).round(1)
            if total_ff:
                corsi["FF%"] = (corsi["FF_attempts"] / total_ff * 100).round(1)
            st.subheader("Shot Attempts by Team (Corsi / Fenwick)")
            st.dataframe(corsi, use_container_width=True)

        # Shot location scatter
        st.subheader("Shot Locations")
        loc = sa.dropna(subset=["x_coord", "y_coord"])
        if not loc.empty:
            scatter = loc[["x_coord", "y_coord", "event_type"]].copy()
            # Small jitter so overlapping points don't fully collide
            st.scatter_chart(
                scatter, x="x_coord", y="y_coord", color="event_type",
                height=350, use_container_width=True,
            )
            st.caption("Rink coordinates: x = ±100 (ends), y = ±42.5 (sides).")

        # Goals by period
        if "period" in sa.columns:
            by_period = (
                sa[sa["event_type"] == "goal"]
                .groupby("period").size()
                .rename("goals").to_frame()
            )
            if not by_period.empty:
                st.subheader("Goals by Period")
                st.bar_chart(by_period)


# ── Tab 6: Momentum ────────────────────────────────────────────────────────────
with tab6:
    st.subheader("Team Momentum (Goals per 5-min window)")
    if mom.empty:
        st.info("Momentum data not yet available.")
    else:
        if "window_start" in mom.columns:
            mom["window_start"] = pd.to_datetime(mom["window_start"])
            pivot = mom.pivot_table(index="window_start", columns="team_abbrev", values="momentum_score", aggfunc="sum").fillna(0)
            st.line_chart(pivot)
            st.dataframe(mom.sort_values("window_start", ascending=False).reset_index(drop=True), use_container_width=True)


# ── Tab 7: Penalties ───────────────────────────────────────────────────────────
with tab7:
    st.subheader("Penalty Summary")
    if pen.empty:
        st.info("No penalty data yet.")
    else:
        display_cols = [c for c in ["penalty_type", "penalty_count", "total_penalty_minutes", "unique_offenders"] if c in pen.columns]
        sort_col = "total_penalty_minutes" if "total_penalty_minutes" in display_cols else display_cols[0]
        st.dataframe(pen[display_cols].sort_values(sort_col, ascending=False).reset_index(drop=True), use_container_width=True)
        if "total_penalty_minutes" in pen.columns and "penalty_type" in pen.columns:
            st.subheader("Penalty Minutes by Type")
            st.bar_chart(pen.set_index("penalty_type")["total_penalty_minutes"])


# ── Tab 8: Player stats ────────────────────────────────────────────────────────
with tab8:
    st.subheader("Player Stats")
    if stats.empty:
        st.info("No player stats yet.")
    else:
        ps = stats.copy()
        ps["player_name"] = ps["player_id"].apply(lambda x: resolve(x, players))

        pos_options = sorted(ps["position"].dropna().unique().tolist())
        pos_filter  = st.multiselect("Filter by position", pos_options, default=pos_options)
        ps = ps[ps["position"].isin(pos_filter)]

        # Derived fields
        if "shots" in ps.columns and "goals" in ps.columns:
            ps["shooting_pct"] = (
                ps["goals"] / ps["shots"].replace(0, float("nan")) * 100
            ).round(1)
        if "faceoff_pct" in ps.columns:
            ps["fo_pct"] = (ps["faceoff_pct"] * 100).round(1)
        if "save_pct" in ps.columns:
            ps["sv_pct"] = (ps["save_pct"] * 100).round(2)
        if "toi_seconds" in ps.columns and "points" in ps.columns:
            toi_hours = ps["toi_seconds"] / 3600.0
            ps["p_per_60"] = (ps["points"] / toi_hours.replace(0, float("nan"))).round(2)
        if "toi_seconds" in ps.columns:
            ps["toi"] = ps["toi_seconds"].apply(fmt_toi)

        display_cols = [c for c in [
            "player_name", "team_abbrev", "position",
            "goals", "assists", "points", "plus_minus",
            "shots", "shooting_pct", "p_per_60",
            "hits", "blocked_shots", "pim",
            "giveaways", "takeaways",
            "faceoff_wins", "faceoff_attempts", "fo_pct",
            "saves", "shots_against", "sv_pct",
            "toi",
        ] if c in ps.columns]
        sort_col = "points" if "points" in display_cols else display_cols[0]
        st.dataframe(
            ps[display_cols].sort_values(sort_col, ascending=False).reset_index(drop=True),
            use_container_width=True,
            height=500,
        )


# ── Tab 9: Season Standings & Leaders ──────────────────────────────────────────
with tab9:
    st.subheader("Season Standings")
    season_for_standings = None
    if selected_season_label not in ("All seasons", LIVE_ONLY_LABEL):
        matched_s = [s for s in all_seasons if season_label(s) == selected_season_label]
        if matched_s:
            season_for_standings = matched_s[0]

    standings = load_season_team_stats(season_for_standings)
    if standings.empty:
        st.info("No standings data available.")
    else:
        st.caption(
            f"Season: {selected_season_label}. "
            "Points: 2 per win, 1 per OT/SO loss. Diff = GF − GA."
        )
        st.dataframe(
            standings.rename(columns={
                "team": "Team", "gp": "GP", "wins": "W",
                "reg_losses": "L", "ot_losses": "OTL",
                "gf": "GF", "ga": "GA", "diff": "Diff", "pts": "Pts",
            }),
            use_container_width=True, hide_index=True,
        )

    st.subheader("Top Scorers")
    leaders = load_season_player_leaders(season_for_standings, limit=50)
    if leaders.empty:
        st.info("No player leaders available.")
    else:
        ldf = leaders.copy()
        if "toi_seconds" in ldf.columns:
            toi_hours = ldf["toi_seconds"] / 3600.0
            ldf["p_per_60"] = (ldf["points"] / toi_hours.replace(0, float("nan"))).round(2)
            ldf["toi"] = ldf["toi_seconds"].apply(fmt_toi)
        if "shots" in ldf.columns and "goals" in ldf.columns:
            ldf["shooting_pct"] = (
                ldf["goals"] / ldf["shots"].replace(0, float("nan")) * 100
            ).round(1)
        display = [c for c in [
            "player_name", "team_abbrev", "position", "gp",
            "goals", "assists", "points", "shooting_pct", "p_per_60",
            "shots", "hits", "blocks", "pim", "plus_minus", "toi",
        ] if c in ldf.columns]
        st.dataframe(
            ldf[display].sort_values("points", ascending=False).reset_index(drop=True),
            use_container_width=True, height=500,
        )
