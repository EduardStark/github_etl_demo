-- models/facts/fact_pull_requests.sql
--
-- Purpose:
--   One row per pull request per repository.
--   Central fact table for all PR-level KPIs:
--     - PR cycle time (cycle_time_hours)
--     - Weekly merge count (aggregate on week_merged)
--     - Code review coverage (is_reviewed)
--
-- Dimension FKs: repo_key → dim_repositories, author_key → dim_users,
--                date_key → dim_date (date of PR creation, YYYYMMDD)

CREATE TABLE IF NOT EXISTS fact_pull_requests (
    pr_key                  SERIAL          NOT NULL,
    github_pr_id            BIGINT          NOT NULL,   -- GitHub's internal PR node ID (numeric)
    repo_key                INT             NOT NULL,
    author_key              INT,                        -- NULL if user no longer exists
    date_key                INT             NOT NULL,   -- creation date, YYYYMMDD

    pr_number               INT             NOT NULL,
    title                   VARCHAR(1024),
    state                   VARCHAR(20)     NOT NULL,   -- 'open', 'closed', 'merged'
    created_at              TIMESTAMP WITH TIME ZONE    NOT NULL,
    merged_at               TIMESTAMP WITH TIME ZONE,   -- NULL if not merged
    closed_at               TIMESTAMP WITH TIME ZONE,   -- NULL if still open

    lines_added             INT,
    lines_deleted           INT,
    files_changed           INT,
    commits_count           INT,
    comments_count          INT             NOT NULL DEFAULT 0,
    review_comments_count   INT             NOT NULL DEFAULT 0,

    -- Derived KPI columns (computed during transformation)
    cycle_time_hours        NUMERIC(10, 2),             -- NULL if not merged
    is_reviewed             BOOLEAN         NOT NULL DEFAULT FALSE,
    merge_method            VARCHAR(20),                -- 'merge', 'squash', 'rebase', NULL

    loaded_at               TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fact_pull_requests PRIMARY KEY (pr_key),
    CONSTRAINT uq_fact_pull_requests_github_id UNIQUE (github_pr_id, repo_key),
    CONSTRAINT fk_fact_pr_repo   FOREIGN KEY (repo_key)   REFERENCES dim_repositories (repo_key),
    CONSTRAINT fk_fact_pr_author FOREIGN KEY (author_key) REFERENCES dim_users (user_key),
    CONSTRAINT fk_fact_pr_date   FOREIGN KEY (date_key)   REFERENCES dim_date (date_key),
    CONSTRAINT chk_fact_pr_state CHECK (state IN ('open', 'closed', 'merged')),
    CONSTRAINT chk_fact_pr_cycle_time CHECK (cycle_time_hours IS NULL OR cycle_time_hours >= 0),
    CONSTRAINT chk_fact_pr_merge_method CHECK (
        merge_method IS NULL OR merge_method IN ('merge', 'squash', 'rebase')
    )
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_pr_repo_key
    ON fact_pull_requests (repo_key);

CREATE INDEX IF NOT EXISTS idx_fact_pr_author_key
    ON fact_pull_requests (author_key);

CREATE INDEX IF NOT EXISTS idx_fact_pr_date_key
    ON fact_pull_requests (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_pr_merged_at
    ON fact_pull_requests (merged_at)
    WHERE merged_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fact_pr_state
    ON fact_pull_requests (state);
