-- models/dimensions/dim_users.sql
--
-- Purpose:
--   One record per unique GitHub user seen as a PR author or reviewer.
--   Upserted on every pipeline run using github_user_id as the natural key.
--   user_key (surrogate) is used as FK in fact_pull_requests and fact_reviews.

CREATE TABLE IF NOT EXISTS dim_users (
    user_key        SERIAL          NOT NULL,
    github_user_id  BIGINT          NOT NULL,   -- GitHub's internal numeric user ID
    login           VARCHAR(255)    NOT NULL,   -- GitHub username / login handle
    display_name    VARCHAR(255),               -- full name if available
    avatar_url      VARCHAR(1024),              -- profile picture URL
    user_type       VARCHAR(50)     NOT NULL DEFAULT 'User',  -- 'User' or 'Bot'
    first_seen_at   TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_dim_users PRIMARY KEY (user_key),
    CONSTRAINT uq_dim_users_github_id UNIQUE (github_user_id),
    CONSTRAINT uq_dim_users_login UNIQUE (login),
    CONSTRAINT chk_dim_users_type CHECK (user_type IN ('User', 'Bot', 'Organization'))
);

-- Index to support login-based lookups during dimension key resolution
CREATE INDEX IF NOT EXISTS idx_dim_users_login
    ON dim_users (login);

-- Upsert helper: called by the warehouse asset with new/updated user records.
-- Usage:
--   INSERT INTO dim_users (github_user_id, login, display_name, avatar_url, user_type)
--   VALUES (...)
--   ON CONFLICT (github_user_id) DO UPDATE SET
--       login        = EXCLUDED.login,
--       display_name = EXCLUDED.display_name,
--       avatar_url   = EXCLUDED.avatar_url,
--       user_type    = EXCLUDED.user_type,
--       updated_at   = NOW();
