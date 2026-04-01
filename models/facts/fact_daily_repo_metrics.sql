-- models/facts/fact_daily_repo_metrics.sql
--
-- Purpose:
--   Daily snapshot — one row per (repository, day) captured at pipeline run time.
--   Used for the test file count trend KPI and general repo health tracking.
--   Follows an "insert-or-replace" pattern: UNIQUE (repo_key, date_key) ensures
--   re-running the pipeline on the same day overwrites the earlier snapshot.

CREATE TABLE IF NOT EXISTS fact_daily_repo_metrics (
    metric_key              SERIAL          NOT NULL,
    repo_key                INT             NOT NULL,
    date_key                INT             NOT NULL,   -- snapshot date, YYYYMMDD

    -- PR activity counters (point-in-time snapshot for the day)
    open_prs                INT             NOT NULL DEFAULT 0,
    merged_prs              INT             NOT NULL DEFAULT 0,

    -- Commit and contributor activity
    total_commits           INT             NOT NULL DEFAULT 0,
    active_contributors     INT             NOT NULL DEFAULT 0,  -- distinct authors with commits

    -- Test file health (KPI: test file count trend)
    test_file_count         INT             NOT NULL DEFAULT 0,
    production_file_count   INT             NOT NULL DEFAULT 0,
    test_coverage_ratio     NUMERIC(6, 4),              -- test_file_count / NULLIF(total, 0)

    loaded_at               TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fact_daily_repo_metrics PRIMARY KEY (metric_key),
    CONSTRAINT uq_fact_daily_repo_metrics UNIQUE (repo_key, date_key),
    CONSTRAINT fk_fact_daily_repo   FOREIGN KEY (repo_key) REFERENCES dim_repositories (repo_key),
    CONSTRAINT fk_fact_daily_date   FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT chk_fact_daily_ratio CHECK (
        test_coverage_ratio IS NULL
        OR (test_coverage_ratio >= 0 AND test_coverage_ratio <= 1)
    )
);

-- Indexes for time-series queries and repo-level trend analysis
CREATE INDEX IF NOT EXISTS idx_fact_daily_repo_key
    ON fact_daily_repo_metrics (repo_key);

CREATE INDEX IF NOT EXISTS idx_fact_daily_date_key
    ON fact_daily_repo_metrics (date_key);
