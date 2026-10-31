"""
Silver parsing shared by the nightly batch (src/batch/batch_silver.py) and the
live pipeline (src/live/live_pipeline.py), so a game is turned into silver
rows by exactly the same code whichever path it arrives on.

Silver is normalised into three tables, each with the grain the API data has:

  nhl.silver.games         one row per game (also games with no plays) —
                           game context, home/away teams, snapshot lineage
  nhl.silver.game_rosters  one row per (game_id, player_id) — who dressed,
                           with names/position/sweater as of that game
  nhl.silver.nhl_plays     one row per (game_id, event_id) — the play itself;
                           players by id only (names live in game_rosters)

Pure Python — no Spark session needed at import time.
"""

from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType,
    DateType, TimestampType,
)

GAMES_TABLE   = "nhl.silver.games"
ROSTERS_TABLE = "nhl.silver.game_rosters"
PLAYS_TABLE   = "nhl.silver.nhl_plays"
FINAL_STATES = {"OFF", "FINAL"}

# ----------------------------------------------------------------------
# Type helpers
# ----------------------------------------------------------------------
def _int(v):
    return int(v) if v is not None else None

def _long(v):
    return int(v) if v is not None else None

def _bool(v):
    return bool(v) if v is not None else None

def _str(v):
    return str(v) if v is not None else None

def _date(v):
    if v is None:
        return None
    if isinstance(v, str):
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    return v

def _ts(v):
    if v is None:
        return None
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
    return v

def _localized(field):
    """Extract the 'default' locale from an NHL API localised string object."""
    if field is None:
        return None
    if isinstance(field, dict):
        return field.get("default")
    return str(field)

# ----------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------
# One row per game. Snapshot lineage lives here only: batch and live both
# replace a game's rows in all three tables at once, so every row of a game
# comes from the same snapshot.
GAMES_SCHEMA = StructType([
    StructField("game_id",                  LongType()),
    StructField("season",                   IntegerType()),
    StructField("game_type",                IntegerType()),
    StructField("limited_scoring",          BooleanType()),
    StructField("game_date",                DateType()),
    StructField("venue",                    StringType()),
    StructField("venue_location",           StringType()),
    StructField("start_time_utc",           TimestampType()),
    StructField("eastern_utc_offset",       StringType()),
    StructField("venue_utc_offset",         StringType()),
    StructField("game_state",               StringType()),
    StructField("game_schedule_state",      StringType()),
    StructField("away_team_id",             IntegerType()),
    StructField("away_team_abbrev",         StringType()),
    StructField("away_team_name",           StringType()),
    StructField("away_team_place_name",     StringType()),
    StructField("away_team_logo",           StringType()),
    StructField("away_team_dark_logo",      StringType()),
    StructField("away_final_score",         IntegerType()),
    StructField("away_final_sog",           IntegerType()),
    StructField("home_team_id",             IntegerType()),
    StructField("home_team_abbrev",         StringType()),
    StructField("home_team_name",           StringType()),
    StructField("home_team_place_name",     StringType()),
    StructField("home_team_logo",           StringType()),
    StructField("home_team_dark_logo",      StringType()),
    StructField("home_final_score",         IntegerType()),
    StructField("home_final_sog",           IntegerType()),
    StructField("last_period_type",         StringType()),
    StructField("reg_periods",              IntegerType()),
    StructField("special_event",            StringType()),
    # Lineage — silver is written by batch_silver ('batch') and the live pipeline ('live')
    StructField("source",                   StringType()),
    StructField("is_final",                 BooleanType()),
    StructField("fetched_at",               TimestampType()),   # when the API was called
    StructField("ingestion_timestamp",      TimestampType()),   # when silver was written
])

# One row per player who dressed for the game (rosterSpots; empty for most
# pre-2010 games).
ROSTERS_SCHEMA = StructType([
    StructField("game_id",                  LongType()),
    StructField("player_id",                LongType()),
    StructField("team_id",                  IntegerType()),
    StructField("first_name",               StringType()),
    StructField("last_name",                StringType()),
    StructField("position_code",            StringType()),
    StructField("sweater_number",           IntegerType()),
    StructField("headshot_url",             StringType()),
    StructField("source",                   StringType()),      # needed by live replaceWhere
    StructField("ingestion_timestamp",      TimestampType()),
])

# One row per play. Game context is in GAMES_SCHEMA, names in ROSTERS_SCHEMA.
PLAYS_SCHEMA = StructType([
    StructField("game_id",                  LongType()),
    StructField("event_id",                 IntegerType()),
    StructField("period_number",            IntegerType()),
    StructField("period_type",              StringType()),
    StructField("time_in_period",           StringType()),
    StructField("time_remaining",           StringType()),
    StructField("type_code",                IntegerType()),
    StructField("type_desc_key",            StringType()),
    StructField("sort_order",               IntegerType()),
    StructField("x_coord",                  IntegerType()),
    StructField("y_coord",                  IntegerType()),
    StructField("zone_code",                StringType()),
    StructField("home_team_defending_side", StringType()),
    StructField("event_owner_team_id",      IntegerType()),
    StructField("shot_type",                StringType()),
    StructField("scoring_player_total",     IntegerType()),
    StructField("assist1_player_total",     IntegerType()),
    StructField("assist2_player_total",     IntegerType()),
    StructField("assist3_player_total",     IntegerType()),
    StructField("away_score",               IntegerType()),
    StructField("home_score",               IntegerType()),
    StructField("away_sog",                 IntegerType()),
    StructField("home_sog",                 IntegerType()),
    StructField("penalty_type_code",        StringType()),
    StructField("penalty_desc_key",         StringType()),
    StructField("penalty_duration",         IntegerType()),
    StructField("stoppage_reason",          StringType()),
    StructField("secondary_stoppage_reason", StringType()),
    StructField("situation_code",           StringType()),
    # Player ids — names are in game_rosters (NULL for pre-API era games)
    StructField("shooting_player_id",       LongType()),
    StructField("goalie_in_net_id",         LongType()),
    StructField("scoring_player_id",        LongType()),
    StructField("assist1_player_id",        LongType()),
    StructField("assist2_player_id",        LongType()),
    StructField("assist3_player_id",        LongType()),
    StructField("hitting_player_id",        LongType()),
    StructField("hittee_player_id",         LongType()),
    StructField("blocking_player_id",       LongType()),
    StructField("winning_player_id",        LongType()),
    StructField("losing_player_id",         LongType()),
    StructField("committed_by_player_id",   LongType()),
    StructField("drawn_by_player_id",       LongType()),
    StructField("served_by_player_id",      LongType()),
    StructField("player_id",                LongType()),
    # Media / video clips
    StructField("highlight_clip",               LongType()),
    StructField("highlight_clip_fr",            LongType()),
    StructField("highlight_clip_sharing_url",    StringType()),
    StructField("highlight_clip_sharing_url_fr", StringType()),
    StructField("discrete_clip",                LongType()),
    StructField("discrete_clip_fr",             LongType()),
    StructField("ppt_replay_url",               StringType()),
    StructField("source",                   StringType()),      # needed by live replaceWhere
    StructField("ingestion_timestamp",      TimestampType()),
])

# ----------------------------------------------------------------------
# Parse — one play-by-play response into rows for the three tables
# ----------------------------------------------------------------------
def parse_game(data: dict, fetched_at: datetime, source: str, ingestion_ts: datetime) -> dict:
    """The game's GAMES_SCHEMA row."""
    away    = data.get("awayTeam", {}) or {}
    home    = data.get("homeTeam", {}) or {}
    outcome = data.get("gameOutcome", {}) or {}

    return {
        "game_id":              _long(data.get("id")),
        "season":               _int(data.get("season")),
        "game_type":            _int(data.get("gameType")),
        "limited_scoring":      _bool(data.get("limitedScoring")),
        "game_date":            _date(data.get("gameDate")),
        "venue":                _str(_localized(data.get("venue"))),
        "venue_location":       _str(_localized(data.get("venueLocation"))),
        "start_time_utc":       _ts(data.get("startTimeUTC")),
        "eastern_utc_offset":   _str(data.get("easternUTCOffset")),
        "venue_utc_offset":     _str(data.get("venueUTCOffset")),
        "game_state":           _str(data.get("gameState")),
        "game_schedule_state":  _str(data.get("gameScheduleState")),
        "away_team_id":         _int(away.get("id")),
        "away_team_abbrev":     _str(away.get("abbrev")),
        "away_team_name":       _str(_localized(away.get("commonName"))),
        "away_team_place_name": _str(_localized(away.get("placeName"))),
        "away_team_logo":       _str(away.get("logo")),
        "away_team_dark_logo":  _str(away.get("darkLogo")),
        "away_final_score":     _int(away.get("score")),
        "away_final_sog":       _int(away.get("sog")),
        "home_team_id":         _int(home.get("id")),
        "home_team_abbrev":     _str(home.get("abbrev")),
        "home_team_name":       _str(_localized(home.get("commonName"))),
        "home_team_place_name": _str(_localized(home.get("placeName"))),
        "home_team_logo":       _str(home.get("logo")),
        "home_team_dark_logo":  _str(home.get("darkLogo")),
        "home_final_score":     _int(home.get("score")),
        "home_final_sog":       _int(home.get("sog")),
        "last_period_type":     _str(outcome.get("lastPeriodType")),
        "reg_periods":          _int(data.get("regPeriods")),
        "special_event":        _str(_localized(data.get("specialEvent"))),
        "source":               source,
        "is_final":             data.get("gameState") in FINAL_STATES,
        "fetched_at":           fetched_at,
        "ingestion_timestamp":  ingestion_ts,
    }


def parse_roster(data: dict, source: str, ingestion_ts: datetime) -> list:
    """One ROSTERS_SCHEMA row per player in rosterSpots."""
    game_id = _long(data.get("id"))
    rows = {}
    for spot in data.get("rosterSpots") or []:
        player_id = _long(spot.get("playerId"))
        if not player_id:
            continue
        rows[player_id] = {   # keyed by player, so a duplicated spot can't break the grain
            "game_id":             game_id,
            "player_id":           player_id,
            "team_id":             _int(spot.get("teamId")),
            "first_name":          _str(_localized(spot.get("firstName"))),
            "last_name":           _str(_localized(spot.get("lastName"))),
            "position_code":       _str(spot.get("positionCode")),
            "sweater_number":      _int(spot.get("sweaterNumber")),
            "headshot_url":        _str(spot.get("headshot")),
            "source":              source,
            "ingestion_timestamp": ingestion_ts,
        }
    return list(rows.values())


def parse_plays(data: dict, source: str, ingestion_ts: datetime) -> list:
    """One PLAYS_SCHEMA row per play."""
    game_id = _long(data.get("id"))
    rows = []
    for play in data.get("plays") or []:
        d      = play.get("details") or {}
        period = play.get("periodDescriptor") or {}
        rows.append({
            "game_id":                  game_id,
            "event_id":                 _int(play.get("eventId")),
            "period_number":            _int(period.get("number")),
            "period_type":              _str(period.get("periodType")),
            "time_in_period":           _str(play.get("timeInPeriod")),
            "time_remaining":           _str(play.get("timeRemaining")),
            "type_code":                _int(play.get("typeCode")),
            "type_desc_key":            _str(play.get("typeDescKey")),
            "sort_order":               _int(play.get("sortOrder")),
            "x_coord":                  _int(d.get("xCoord")),
            "y_coord":                  _int(d.get("yCoord")),
            "zone_code":                _str(d.get("zoneCode")),
            "home_team_defending_side": _str(play.get("homeTeamDefendingSide")),
            "event_owner_team_id":      _int(d.get("eventOwnerTeamId")),
            "shot_type":                _str(d.get("shotType")),
            "scoring_player_total":     _int(d.get("scoringPlayerTotal")),
            "assist1_player_total":     _int(d.get("assist1PlayerTotal")),
            "assist2_player_total":     _int(d.get("assist2PlayerTotal")),
            "assist3_player_total":     _int(d.get("assist3PlayerTotal")),
            "away_score":               _int(d.get("awayScore")),
            "home_score":               _int(d.get("homeScore")),
            "away_sog":                 _int(d.get("awaySOG")),
            "home_sog":                 _int(d.get("homeSOG")),
            "penalty_type_code":        _str(d.get("typeCode")),
            "penalty_desc_key":         _str(d.get("descKey")),
            "penalty_duration":         _int(d.get("duration")),
            "stoppage_reason":          _str(d.get("reason")),
            "secondary_stoppage_reason": _str(d.get("secondaryReason")),
            "situation_code":            _str(play.get("situationCode")),
            # Player ids
            "shooting_player_id":        _long(d.get("shootingPlayerId")),
            "goalie_in_net_id":          _long(d.get("goalieInNetId")),
            "scoring_player_id":         _long(d.get("scoringPlayerId")),
            "assist1_player_id":         _long(d.get("assist1PlayerId")),
            "assist2_player_id":         _long(d.get("assist2PlayerId")),
            "assist3_player_id":         _long(d.get("assist3PlayerId")),
            "hitting_player_id":         _long(d.get("hittingPlayerId")),
            "hittee_player_id":          _long(d.get("hitteePlayerId")),
            "blocking_player_id":        _long(d.get("blockingPlayerId")),
            "winning_player_id":         _long(d.get("winningPlayerId")),
            "losing_player_id":          _long(d.get("losingPlayerId")),
            "committed_by_player_id":    _long(d.get("committedByPlayerId")),
            "drawn_by_player_id":        _long(d.get("drawnByPlayerId")),
            "served_by_player_id":       _long(d.get("servedByPlayerId")),
            "player_id":                 _long(d.get("playerId")),
            # Media / video clips
            "highlight_clip":                _long(d.get("highlightClip")),
            "highlight_clip_fr":             _long(d.get("highlightClipFr")),
            "highlight_clip_sharing_url":     _str(d.get("highlightClipSharingUrl")),
            "highlight_clip_sharing_url_fr":  _str(d.get("highlightClipSharingUrlFr")),
            "discrete_clip":                 _long(d.get("discreteClip")),
            "discrete_clip_fr":              _long(d.get("discreteClipFr")),
            "ppt_replay_url":                 _str(play.get("pptReplayUrl")),
            "source":                        source,
            "ingestion_timestamp":           ingestion_ts,
        })
    return rows
