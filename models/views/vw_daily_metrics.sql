CREATE OR REPLACE VIEW vw_daily_metrics AS
SELECT
    f.*,
    r.repo_name,
    r.org_name,
    r.full_name AS repo_full_name,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_name,
    d.is_weekend
FROM fact_daily_repo_metrics f
LEFT JOIN dim_repositories r ON f.repo_key = r.repo_key
LEFT JOIN dim_date         d ON f.date_key = d.date_key;
