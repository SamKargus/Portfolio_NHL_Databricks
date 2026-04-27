-- NHL Pipeline PostgreSQL Schema
-- Run with: psql -U samkargus -d nhl_pipeline -f config/schema.sql

-- ── Bronze ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_game_events (
    event_id              TEXT        NOT NULL,
    game_id               BIGINT      NOT NULL,
    season                INT,
    game_type             INT,
    event_type            TEXT,
    period                INT,
    period_type           TEXT,
    time_in_period        TEXT,
    time_remaining        TEXT,
    home_team_id          INT,
    home_team_abbrev      TEXT,
    away_team_id          INT,
    away_team_abbrev      TEXT,
    home_score            INT,
    away_score            INT,
    scoring_player_id     BIGINT,
    assist1_player_id     BIGINT,
    assist2_player_id     BIGINT,
    goalie_in_net_id      BIGINT,
    shot_type             TEXT,
    goal_type             TEXT,
    committed_by_player_id BIGINT,
    drawn_by_player_id    BIGINT,
    penalty_type          TEXT,
    duration_minutes      INT,
    x_coord               DOUBLE PRECISION,
    y_coord               DOUBLE PRECISION,
    zone_code             TEXT,
    hittee_player_id      BIGINT,
    hitting_player_id     BIGINT,
    player_id             BIGINT,
    kafka_timestamp       TIMESTAMPTZ,
    bronze_ingested_at    TIMESTAMPTZ,
    PRIMARY KEY (event_id, game_id)
);

CREATE TABLE IF NOT EXISTS bronze_player_stats (
    stat_id              TEXT        PRIMARY KEY,
    game_id              BIGINT,
    season               INT,
    team_id              INT,
    team_abbrev          TEXT,
    player_id            BIGINT,
    player_name          TEXT,
    position             TEXT,
    sweater_number       INT,
    goals                INT,
    assists              INT,
    points               INT,
    plus_minus           INT,
    pim                  INT,
    hits                 INT,
    blocked_shots        INT,
    shots                INT,
    faceoff_wins         INT,
    faceoff_attempts     INT,
    saves                INT,
    shots_against        INT,
    save_pct             DOUBLE PRECISION,
    goals_against        INT,
    time_on_ice          TEXT,
    power_play_toi       TEXT,
    short_handed_toi     TEXT,
    giveaways            INT,
    takeaways            INT,
    kafka_timestamp      TIMESTAMPTZ,
    bronze_ingested_at   TIMESTAMPTZ
);

-- ── Silver ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver_game_events (
    event_id                TEXT        NOT NULL,
    game_id                 BIGINT      NOT NULL,
    season                  INT,
    game_type               INT,
    event_type              TEXT,
    period                  INT,
    period_type             TEXT,
    time_in_period          TEXT,
    time_remaining          TEXT,
    time_in_period_secs     INT,
    time_remaining_secs     INT,
    game_second             INT,
    home_team_id            INT,
    home_team_abbrev        TEXT,
    away_team_id            INT,
    away_team_abbrev        TEXT,
    home_score              INT,
    away_score              INT,
    score_differential      INT,
    zone_code               TEXT,
    x_coord                 DOUBLE PRECISION,
    y_coord                 DOUBLE PRECISION,
    scoring_player_id       BIGINT,
    scoring_player_total    INT,
    assist1_player_id       BIGINT,
    assist1_player_total    INT,
    assist2_player_id       BIGINT,
    assist2_player_total    INT,
    goalie_in_net_id        BIGINT,
    shot_type               TEXT,
    goal_type               TEXT,
    penalty_type            TEXT,
    duration_minutes        INT,
    committed_by_player_id  BIGINT,
    drawn_by_player_id      BIGINT,
    hittee_player_id        BIGINT,
    hitting_player_id       BIGINT,
    player_id               BIGINT,
    is_overtime             BOOLEAN,
    is_penalty_shot         BOOLEAN,
    kafka_timestamp         TIMESTAMPTZ,
    silver_processed_at     TIMESTAMPTZ,
    PRIMARY KEY (event_id, game_id)
);

CREATE TABLE IF NOT EXISTS silver_player_stats (
    stat_id              TEXT        PRIMARY KEY,
    game_id              BIGINT,
    season               INT,
    team_id              INT,
    team_abbrev          TEXT,
    player_id            BIGINT,
    player_name          TEXT,
    position             TEXT,
    sweater_number       INT,
    goals                INT,
    assists              INT,
    points               INT,
    plus_minus           INT,
    pim                  INT,
    hits                 INT,
    blocked_shots        INT,
    shots                INT,
    faceoff_wins         INT,
    faceoff_attempts     INT,
    faceoff_pct          DOUBLE PRECISION,
    saves                INT,
    shots_against        INT,
    save_pct             DOUBLE PRECISION,
    goals_against        INT,
    toi_seconds          INT,
    giveaways            INT,
    takeaways            INT,
    kafka_timestamp      TIMESTAMPTZ,
    silver_processed_at  TIMESTAMPTZ
);

-- ── Gold ───────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold_player_scoring (
    game_id              BIGINT      NOT NULL,
    player_id            BIGINT      NOT NULL,
    goals                INT         DEFAULT 0,
    primary_assists      INT         DEFAULT 0,
    secondary_assists    INT         DEFAULT 0,
    total_assists        INT         DEFAULT 0,
    points               INT         DEFAULT 0,
    last_event_at        TIMESTAMPTZ,
    gold_updated_at      TIMESTAMPTZ,
    PRIMARY KEY (game_id, player_id)
);

CREATE TABLE IF NOT EXISTS gold_team_momentum (
    game_id              BIGINT      NOT NULL,
    team_abbrev          TEXT        NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ,
    momentum_score       INT         DEFAULT 0,
    gold_updated_at      TIMESTAMPTZ,
    PRIMARY KEY (game_id, team_abbrev, window_start)
);

CREATE TABLE IF NOT EXISTS gold_penalty_summary (
    game_id                  BIGINT  NOT NULL,
    penalty_type             TEXT    NOT NULL,
    penalty_count            INT     DEFAULT 0,
    total_penalty_minutes    INT     DEFAULT 0,
    unique_offenders         BIGINT  DEFAULT 0,
    last_penalty_at          TIMESTAMPTZ,
    gold_updated_at          TIMESTAMPTZ,
    PRIMARY KEY (game_id, penalty_type)
);

-- ── Indexes ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_silver_events_game   ON silver_game_events (game_id);
CREATE INDEX IF NOT EXISTS idx_silver_events_type   ON silver_game_events (event_type);
CREATE INDEX IF NOT EXISTS idx_silver_stats_game    ON silver_player_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_gold_scoring_game    ON gold_player_scoring (game_id);
CREATE INDEX IF NOT EXISTS idx_gold_momentum_game   ON gold_team_momentum (game_id);
CREATE INDEX IF NOT EXISTS idx_gold_penalty_game    ON gold_penalty_summary (game_id);
