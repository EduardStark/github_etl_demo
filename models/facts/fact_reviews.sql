-- models/facts/fact_reviews.sql
--
-- Purpose:
--   One row per review event per pull request.
--   Used for:
--     - Review responsiveness KPI (response_time_hours on the first review)
--     - Reviewer activity analysis
--
-- Dimension FKs: pr_key → fact_pull_requests, reviewer_key → dim_users,
--                date_key → dim_date (date review was submitted, YYYYMMDD)

CREATE TABLE IF NOT EXISTS fact_reviews (
    review_key              SERIAL          NOT NULL,
    github_review_id        BIGINT          NOT NULL,   -- GitHub's internal review ID
    pr_key                  INT             NOT NULL,
    reviewer_key            INT,                        -- NULL if user no longer exists
    date_key                INT             NOT NULL,   -- submission date, YYYYMMDD

    review_state            VARCHAR(30)     NOT NULL,   -- 'APPROVED', 'CHANGES_REQUESTED',
                                                        -- 'COMMENTED', 'DISMISSED'
    submitted_at            TIMESTAMP WITH TIME ZONE    NOT NULL,

    -- KPI columns
    response_time_hours     NUMERIC(10, 2),             -- time from PR creation to this review
    comments_count          INT             NOT NULL DEFAULT 0,
    is_first_review         BOOLEAN         NOT NULL DEFAULT FALSE,  -- TRUE for earliest review on PR

    loaded_at               TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fact_reviews PRIMARY KEY (review_key),
    CONSTRAINT uq_fact_reviews_github_id UNIQUE (github_review_id),
    CONSTRAINT fk_fact_review_pr       FOREIGN KEY (pr_key)       REFERENCES fact_pull_requests (pr_key),
    CONSTRAINT fk_fact_review_reviewer FOREIGN KEY (reviewer_key) REFERENCES dim_users (user_key),
    CONSTRAINT fk_fact_review_date     FOREIGN KEY (date_key)     REFERENCES dim_date (date_key),
    CONSTRAINT chk_fact_review_state CHECK (
        review_state IN ('APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED', 'PENDING')
    ),
    CONSTRAINT chk_fact_review_response_time CHECK (
        response_time_hours IS NULL OR response_time_hours >= 0
    )
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_reviews_pr_key
    ON fact_reviews (pr_key);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_reviewer_key
    ON fact_reviews (reviewer_key);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_date_key
    ON fact_reviews (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_first_review
    ON fact_reviews (pr_key, is_first_review)
    WHERE is_first_review = TRUE;
