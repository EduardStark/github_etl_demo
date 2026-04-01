-- models/dimensions/dim_date.sql
--
-- Purpose:
--   Static calendar dimension covering 2020-01-01 to 2030-12-31.
--   Populated once via generate_series; never modified by pipeline runs.
--   date_key uses YYYYMMDD integer format for fast range filtering in fact joins.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT             NOT NULL,   -- YYYYMMDD, e.g. 20240315
    full_date       DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,   -- 1–4
    month           SMALLINT        NOT NULL,   -- 1–12
    month_name      VARCHAR(10)     NOT NULL,   -- 'January' … 'December'
    week_of_year    SMALLINT        NOT NULL,   -- ISO week 1–53
    day_of_week     SMALLINT        NOT NULL,   -- ISO: 1=Monday … 7=Sunday
    day_name        VARCHAR(10)     NOT NULL,   -- 'Monday' … 'Sunday'
    is_weekend      BOOLEAN         NOT NULL,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date)
);

-- Populate once; ON CONFLICT DO NOTHING makes re-runs safe.
INSERT INTO dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                         AS date_key,
    d                                                   AS full_date,
    EXTRACT(YEAR  FROM d)::SMALLINT                     AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT                   AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT                     AS month,
    TO_CHAR(d, 'Month')                                 AS month_name,
    EXTRACT(WEEK  FROM d)::SMALLINT                     AS week_of_year,
    EXTRACT(ISODOW FROM d)::SMALLINT                    AS day_of_week,
    TO_CHAR(d, 'Day')                                   AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7)                    AS is_weekend
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS gs(d)
ON CONFLICT (date_key) DO NOTHING;
