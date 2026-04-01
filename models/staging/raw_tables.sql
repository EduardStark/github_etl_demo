-- models/staging/raw_tables.sql
--
-- Purpose:
--   Staging tables for raw data extracted from the GitHub API.
--   Raw records land here before any transformation or validation.
--   This decouples extraction failures from warehouse corruption:
--   transformations can be replayed without re-hitting the API.
--
--   Schema: staging (created by docker/postgres/init.sql)
--   Retention: rows can be truncated after successful warehouse load,
--              or retained for debugging/replay purposes.

-- ---------------------------------------------------------------------------
-- staging.raw_pull_requests
-- One row per raw PR record as returned by the GitHub REST or GraphQL API.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.raw_pull_requests (
    id                  BIGSERIAL       NOT NULL,

    -- Source identifiers
    github_pr_id        BIGINT          NOT NULL,   -- GitHub's numeric PR ID
    github_repo_id      BIGINT          NOT NULL,   -- GitHub's numeric repo ID
    repo_full_name      VARCHAR(512)    NOT NULL,   -- 'org/repo'
    pr_number           INT             NOT NULL,

    -- Core PR fields
    title               VARCHAR(1024),
    state               VARCHAR(20),                -- 'open', 'closed'
    draft               BOOLEAN,
    author_login        VARCHAR(255),
    author_id           BIGINT,
    base_branch         VARCHAR(255),
    head_branch         VARCHAR(255),
    merge_commit_sha    VARCHAR(40),
    merge_method        VARCHAR(20),

    -- Timestamps (stored as text to preserve raw API format; cast during transform)
    created_at          TEXT,
    updated_at          TEXT,
    merged_at           TEXT,
    closed_at           TEXT,

    -- Metrics
    lines_added         INT,
    lines_deleted       INT,
    changed_files       INT,
    commits             INT,
    comments            INT,
    review_comments     INT,

    -- Full raw payload for debugging / schema evolution
    raw_payload         JSONB,

    -- Pipeline metadata
    extracted_at        TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    pipeline_run_id     VARCHAR(255),               -- Dagster run ID for traceability

    CONSTRAINT pk_staging_raw_pull_requests PRIMARY KEY (id),
    CONSTRAINT uq_staging_raw_pr UNIQUE (github_pr_id, github_repo_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_raw_pr_repo
    ON staging.raw_pull_requests (repo_full_name);

CREATE INDEX IF NOT EXISTS idx_staging_raw_pr_extracted
    ON staging.raw_pull_requests (extracted_at);

-- ---------------------------------------------------------------------------
-- staging.raw_reviews
-- One row per review event as returned by the GitHub REST API.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.raw_reviews (
    id                  BIGSERIAL       NOT NULL,

    -- Source identifiers
    github_review_id    BIGINT          NOT NULL,
    github_pr_id        BIGINT          NOT NULL,   -- links back to raw_pull_requests
    github_repo_id      BIGINT          NOT NULL,
    repo_full_name      VARCHAR(512)    NOT NULL,
    pr_number           INT             NOT NULL,

    -- Review fields
    reviewer_login      VARCHAR(255),
    reviewer_id         BIGINT,
    state               VARCHAR(30),                -- 'APPROVED', 'CHANGES_REQUESTED', etc.
    body                TEXT,                       -- review body text
    commit_id           VARCHAR(40),
    html_url            VARCHAR(1024),

    -- Timestamps (stored as text; cast during transform)
    submitted_at        TEXT,

    -- Individual review comment count (from review object, not thread comments)
    comments_count      INT             NOT NULL DEFAULT 0,

    -- Full raw payload for debugging / schema evolution
    raw_payload         JSONB,

    -- Pipeline metadata
    extracted_at        TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),
    pipeline_run_id     VARCHAR(255),

    CONSTRAINT pk_staging_raw_reviews PRIMARY KEY (id),
    CONSTRAINT uq_staging_raw_review UNIQUE (github_review_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_raw_review_pr
    ON staging.raw_reviews (github_pr_id, github_repo_id);

CREATE INDEX IF NOT EXISTS idx_staging_raw_review_extracted
    ON staging.raw_reviews (extracted_at);
