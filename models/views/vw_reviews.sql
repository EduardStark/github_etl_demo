CREATE OR REPLACE VIEW vw_reviews AS
SELECT
    f.*,
    r.repo_name,
    r.org_name,
    r.full_name    AS repo_full_name,
    u.login        AS reviewer_login,
    u.display_name AS reviewer_name,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_name
FROM fact_reviews f
LEFT JOIN fact_pull_requests pr ON f.pr_key        = pr.pr_key
LEFT JOIN dim_repositories   r  ON pr.repo_key     = r.repo_key
LEFT JOIN dim_users          u  ON f.reviewer_key  = u.user_key
LEFT JOIN dim_date           d  ON f.date_key      = d.date_key;
