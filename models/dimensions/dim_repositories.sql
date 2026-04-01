-- models/dimensions/dim_repositories.sql
--
-- Purpose:
--   One record per GitHub repository tracked by the pipeline.
--   Upserted on every pipeline run using github_repo_id as the natural key.
--   repo_key (surrogate) is used as FK in all fact tables.

CREATE TABLE IF NOT EXISTS dim_repositories (
    repo_key        SERIAL          NOT NULL,
    github_repo_id  BIGINT          NOT NULL,   -- GitHub's internal numeric repo ID
    repo_name       VARCHAR(255)    NOT NULL,   -- short name, e.g. 'my-repo'
    org_name        VARCHAR(255)    NOT NULL,   -- owning org or user login
    full_name       VARCHAR(512)    NOT NULL,   -- 'org/repo'
    default_branch  VARCHAR(100)    NOT NULL DEFAULT 'main',
    language        VARCHAR(100),               -- primary language reported by GitHub
    is_private      BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP WITH TIME ZONE,
    updated_at      TIMESTAMP WITH TIME ZONE,
    loaded_at       TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_dim_repositories PRIMARY KEY (repo_key),
    CONSTRAINT uq_dim_repositories_github_id UNIQUE (github_repo_id),
    CONSTRAINT uq_dim_repositories_full_name UNIQUE (full_name)
);

-- Indexes to support fact-table FK lookups
CREATE INDEX IF NOT EXISTS idx_dim_repositories_full_name
    ON dim_repositories (full_name);

-- Upsert helper: called by the warehouse asset with new/updated repo records.
-- Usage:
--   INSERT INTO dim_repositories (...) VALUES (...)
--   ON CONFLICT (github_repo_id) DO UPDATE SET
--       repo_name      = EXCLUDED.repo_name,
--       org_name       = EXCLUDED.org_name,
--       full_name      = EXCLUDED.full_name,
--       default_branch = EXCLUDED.default_branch,
--       language       = EXCLUDED.language,
--       is_private     = EXCLUDED.is_private,
--       updated_at     = EXCLUDED.updated_at,
--       loaded_at      = NOW();
