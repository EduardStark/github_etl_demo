"""
scripts/test_load.py

End-to-end load test: mock data → transform → load into PostgreSQL → query KPIs.

Prerequisites:
    docker-compose up -d postgres
    python scripts/init_db.py

Run from the project root:
    python scripts/test_load.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s — %(message)s",
)
# Quiet noisy libraries
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("pg8000").setLevel(logging.WARNING)

from sqlalchemy import create_engine, text

from connectors.github.mock_data import generate_mock_pull_requests, generate_mock_reviews
from connectors.database.loader import DatabaseLoader, _make_engine
from transformers.github.pull_requests import transform_pull_requests
from transformers.github.reviews import transform_reviews, flag_reviewed_prs
from transformers.github.dimensions import extract_dim_repositories, extract_dim_users


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def query_count(engine, table: str) -> int:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
        return row[0]


def check_tables_exist(engine) -> bool:
    """Return True if the star schema tables exist; print hint if not."""
    tables = [
        "dim_date", "dim_repositories", "dim_users",
        "fact_pull_requests", "fact_reviews", "fact_daily_repo_metrics",
    ]
    with engine.connect() as conn:
        missing = []
        for t in tables:
            row = conn.execute(text(
                "SELECT to_regclass(:t)"
            ), {"t": f"public.{t}"}).fetchone()
            if row[0] is None:
                missing.append(t)
    if missing:
        print(f"\n  ERROR: Missing tables: {missing}")
        print("  Run:  python scripts/init_db.py")
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    import os
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("ERROR: DATABASE_URL not set. Check your .env file.")
        sys.exit(1)

    engine = _make_engine(database_url)

    # ------------------------------------------------------------------
    section("1. Checking database connectivity and schema")
    # ------------------------------------------------------------------
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  Connected to PostgreSQL.")
    except Exception as exc:
        print(f"  ERROR: Cannot connect — {exc}")
        print("  Run:  docker-compose up -d postgres")
        sys.exit(1)

    if not check_tables_exist(engine):
        sys.exit(1)
    print("  All tables present.")

    # ------------------------------------------------------------------
    section("2. Generating and transforming mock data")
    # ------------------------------------------------------------------
    REPO = "my-org/test-repo"
    raw_prs     = generate_mock_pull_requests(REPO, count=30)
    raw_reviews = generate_mock_reviews(raw_prs)

    pr_df     = transform_pull_requests(raw_prs)
    review_df = transform_reviews(raw_reviews, pr_df)
    pr_df     = flag_reviewed_prs(pr_df, review_df)
    repo_df   = extract_dim_repositories(raw_prs)
    user_df   = extract_dim_users(raw_prs, raw_reviews)

    print(f"  PRs       : {pr_df.height}")
    print(f"  Reviews   : {review_df.height}")
    print(f"  Repos     : {repo_df.height}")
    print(f"  Users     : {user_df.height}")

    # ------------------------------------------------------------------
    section("3. Loading into PostgreSQL")
    # ------------------------------------------------------------------
    loader = DatabaseLoader(database_url)

    n_repos   = loader.upsert_dim_repositories(repo_df)
    n_users   = loader.upsert_dim_users(user_df)
    n_prs     = loader.load_fact_pull_requests(pr_df)
    n_reviews = loader.load_fact_reviews(review_df)
    n_daily   = loader.load_fact_daily_metrics(pr_df, review_df)

    print(f"  dim_repositories        : {n_repos} row(s)")
    print(f"  dim_users               : {n_users} row(s)")
    print(f"  fact_pull_requests      : {n_prs} row(s)")
    print(f"  fact_reviews            : {n_reviews} row(s)")
    print(f"  fact_daily_repo_metrics : {n_daily} row(s)")

    # ------------------------------------------------------------------
    section("4. Row counts per table")
    # ------------------------------------------------------------------
    tables = [
        "dim_date",
        "dim_repositories",
        "dim_users",
        "fact_pull_requests",
        "fact_reviews",
        "fact_daily_repo_metrics",
        "staging.raw_pull_requests",
        "staging.raw_reviews",
    ]
    for t in tables:
        count = query_count(engine, t)
        print(f"  {t:<35} {count:>6} row(s)")

    # ------------------------------------------------------------------
    section("5. KPI query — avg cycle_time_hours for merged PRs by week")
    # ------------------------------------------------------------------
    kpi_sql = text("""
        SELECT
            TO_CHAR(DATE_TRUNC('week', fpr.merged_at), 'YYYY-"W"IW') AS week,
            COUNT(*)                                                   AS pr_count,
            ROUND(AVG(fpr.cycle_time_hours)::numeric, 2)              AS avg_cycle_hours,
            ROUND((AVG(fpr.cycle_time_hours) / 24.0)::numeric, 2)     AS avg_cycle_days
        FROM fact_pull_requests fpr
        JOIN dim_repositories   dr  ON dr.repo_key  = fpr.repo_key
        WHERE fpr.merged_at  IS NOT NULL
          AND fpr.state       = 'merged'
          AND dr.full_name    = :repo
        GROUP BY DATE_TRUNC('week', fpr.merged_at)
        ORDER BY DATE_TRUNC('week', fpr.merged_at)
    """)

    with engine.connect() as conn:
        rows = conn.execute(kpi_sql, {"repo": REPO}).fetchall()

    if rows:
        print(f"\n  {'Week':<12} {'PRs':>5} {'Avg hours':>12} {'Avg days':>10}")
        print(f"  {'-'*12} {'-'*5} {'-'*12} {'-'*10}")
        for row in rows:
            print(f"  {row[0]:<12} {row[1]:>5} {row[2]:>12} {row[3]:>10}")
    else:
        print("  No merged PRs found — load may have had issues.")

    # ------------------------------------------------------------------
    section("6. Bonus — review responsiveness (first reviews only)")
    # ------------------------------------------------------------------
    resp_sql = text("""
        SELECT
            ROUND(AVG(fr.response_time_hours)::numeric, 2) AS avg_response_hours,
            ROUND(MIN(fr.response_time_hours)::numeric, 2) AS min_response_hours,
            ROUND(MAX(fr.response_time_hours)::numeric, 2) AS max_response_hours,
            COUNT(*)                                        AS first_review_count
        FROM fact_reviews       fr
        JOIN fact_pull_requests fpr ON fpr.pr_key   = fr.pr_key
        JOIN dim_repositories   dr  ON dr.repo_key  = fpr.repo_key
        WHERE fr.is_first_review = TRUE
          AND dr.full_name       = :repo
    """)

    with engine.connect() as conn:
        row = conn.execute(resp_sql, {"repo": REPO}).fetchone()

    if row and row[3]:
        print(f"\n  First reviews   : {row[3]}")
        print(f"  Avg response    : {row[0]} h")
        print(f"  Min response    : {row[1]} h")
        print(f"  Max response    : {row[2]} h")
    else:
        print("  No first-review data found.")

    print()


if __name__ == "__main__":
    main()
