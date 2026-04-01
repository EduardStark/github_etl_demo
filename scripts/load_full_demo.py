"""
scripts/load_full_demo.py

Generates mock data for three qa-automation repositories with distinct activity
profiles, transforms it, and loads everything into PostgreSQL.

Repo profiles
─────────────
  qa-automation/SWUM-test    — high-activity test suite (80 PRs, large diffs,
                               long cycle times, ~85% review coverage, 9 authors)
  qa-automation/test-lib     — shared libraries (40 PRs, focused diffs,
                               very high review coverage ~95%, 4 authors)
  qa-automation/test-config  — environment config (20 PRs, tiny diffs,
                               lower review coverage ~60%, 3 authors)

Prerequisites:
    docker-compose up -d postgres
    python scripts/init_db.py

Run from the project root:
    python scripts/load_full_demo.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s — %(message)s",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("pg8000").setLevel(logging.WARNING)

from sqlalchemy import text

from connectors.github.mock_data import generate_mock_pull_requests, generate_mock_reviews
from connectors.database.loader import DatabaseLoader, _make_engine
from transformers.github.pull_requests import transform_pull_requests
from transformers.github.reviews import transform_reviews, flag_reviewed_prs
from transformers.github.dimensions import extract_dim_repositories, extract_dim_users


# ---------------------------------------------------------------------------
# Repository profiles
# ---------------------------------------------------------------------------

REPO_PROFILES = [
    {
        "repo_full_name":   "qa-automation/SWUM-test",
        "count":            80,
        "days_back":        90,
        "seed":             101,
        "n_users":          9,
        "max_lines_added":  1200,
        "cycle_time_range": (8, 120),
        "review_coverage":  0.85,
    },
    {
        "repo_full_name":   "qa-automation/test-lib",
        "count":            40,
        "days_back":        90,
        "seed":             202,
        "n_users":          4,
        "max_lines_added":  300,
        "cycle_time_range": (4, 48),
        "review_coverage":  0.95,
    },
    {
        "repo_full_name":   "qa-automation/test-config",
        "count":            20,
        "days_back":        90,
        "seed":             303,
        "n_users":          3,
        "max_lines_added":  150,
        "cycle_time_range": (2, 24),
        "review_coverage":  0.60,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")


def check_tables_exist(engine) -> bool:
    tables = [
        "dim_date", "dim_repositories", "dim_users",
        "fact_pull_requests", "fact_reviews", "fact_daily_repo_metrics",
    ]
    with engine.connect() as conn:
        missing = []
        for t in tables:
            row = conn.execute(
                text("SELECT to_regclass(:t)"), {"t": f"public.{t}"}
            ).fetchone()
            if row[0] is None:
                missing.append(t)
    if missing:
        print(f"\n  ERROR: Missing tables: {missing}")
        print("  Run:  python scripts/init_db.py")
        return False
    return True


def clear_demo_data(engine) -> None:
    """Truncate facts and dimension tables (except dim_date) for a clean reload.

    Facts are truncated first to drop FK references, then dimensions.
    dim_date is left intact — it is static and expensive to regenerate.
    """
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_reviews CASCADE"))
        conn.execute(text("TRUNCATE TABLE fact_daily_repo_metrics CASCADE"))
        conn.execute(text("TRUNCATE TABLE fact_pull_requests CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_users CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_repositories CASCADE"))
    print("  Cleared: fact_reviews, fact_daily_repo_metrics, fact_pull_requests, dim_users, dim_repositories")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
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
    section("2. Generating and transforming mock data for all repos")
    # ------------------------------------------------------------------

    all_raw_prs: list[dict] = []
    all_raw_reviews: list[dict] = []

    # Per-repo transformed DataFrames (needed for per-repo stats later)
    repo_data: list[dict] = []

    for profile in REPO_PROFILES:
        repo = profile["repo_full_name"]
        raw_prs = generate_mock_pull_requests(
            repo_full_name=repo,
            count=profile["count"],
            days_back=profile["days_back"],
            seed=profile["seed"],
            n_users=profile["n_users"],
            max_lines_added=profile["max_lines_added"],
            cycle_time_range=profile["cycle_time_range"],
        )
        raw_reviews = generate_mock_reviews(
            raw_prs,
            seed=profile["seed"],
            review_coverage=profile["review_coverage"],
        )

        pr_df     = transform_pull_requests(raw_prs)
        review_df = transform_reviews(raw_reviews, pr_df)
        pr_df     = flag_reviewed_prs(pr_df, review_df)

        all_raw_prs.extend(raw_prs)
        all_raw_reviews.extend(raw_reviews)

        repo_data.append({
            "repo":       repo,
            "raw_prs":    raw_prs,
            "raw_reviews": raw_reviews,
            "pr_df":      pr_df,
            "review_df":  review_df,
        })

        print(f"  {repo:<38}  {pr_df.height:>3} PRs  {review_df.height:>4} reviews")

    # ------------------------------------------------------------------
    section("3. Clearing existing data (facts + dimensions)")
    # ------------------------------------------------------------------
    clear_demo_data(engine)

    # ------------------------------------------------------------------
    section("4. Loading dimensions (all repos combined)")
    # ------------------------------------------------------------------
    repo_dim_df = extract_dim_repositories(all_raw_prs)
    user_dim_df = extract_dim_users(all_raw_prs, all_raw_reviews)

    loader = DatabaseLoader(database_url)
    n_repos = loader.upsert_dim_repositories(repo_dim_df)
    n_users = loader.upsert_dim_users(user_dim_df)

    print(f"  dim_repositories : {n_repos} row(s)")
    print(f"  dim_users        : {n_users} row(s)")

    # ------------------------------------------------------------------
    section("5. Loading facts per repo")
    # ------------------------------------------------------------------
    total_prs = total_reviews = total_daily = 0

    for rd in repo_data:
        n_prs     = loader.load_fact_pull_requests(rd["pr_df"])
        n_reviews = loader.load_fact_reviews(rd["review_df"])
        n_daily   = loader.load_fact_daily_metrics(rd["pr_df"], rd["review_df"])
        total_prs     += n_prs
        total_reviews += n_reviews
        total_daily   += n_daily
        print(
            f"  {rd['repo']:<38}  "
            f"{n_prs:>3} PRs  {n_reviews:>4} reviews  {n_daily:>3} daily rows"
        )

    print(f"\n  Totals — PRs: {total_prs}  Reviews: {total_reviews}  Daily rows: {total_daily}")

    # ------------------------------------------------------------------
    section("6. Per-repo summary")
    # ------------------------------------------------------------------

    summary_sql = text("""
        SELECT
            dr.full_name                                            AS repo,
            COUNT(DISTINCT fpr.pr_key)                             AS pr_count,
            COUNT(DISTINCT fr.review_key)                          AS review_count,
            ROUND(AVG(fpr.cycle_time_hours)::numeric, 1)           AS avg_cycle_hours,
            ROUND(
                100.0 * COUNT(DISTINCT CASE WHEN fpr.is_reviewed THEN fpr.pr_key END)
                / NULLIF(COUNT(DISTINCT fpr.pr_key), 0)
            , 1)                                                    AS review_coverage_pct
        FROM dim_repositories dr
        LEFT JOIN fact_pull_requests fpr ON fpr.repo_key = dr.repo_key
        LEFT JOIN fact_reviews       fr  ON fr.pr_key    = fpr.pr_key
        WHERE dr.org_name = 'qa-automation'
        GROUP BY dr.full_name
        ORDER BY dr.full_name
    """)

    with engine.connect() as conn:
        rows = conn.execute(summary_sql).fetchall()

    col_repo     = "Repository"
    col_prs      = "PRs"
    col_reviews  = "Reviews"
    col_cycle    = "Avg Cycle (h)"
    col_coverage = "Review Coverage"

    print(f"\n  {col_repo:<38}  {col_prs:>5}  {col_reviews:>8}  {col_cycle:>13}  {col_coverage:>15}")
    print(f"  {'-'*38}  {'-'*5}  {'-'*8}  {'-'*13}  {'-'*15}")

    for row in rows:
        repo_name     = row[0] or ""
        pr_count      = row[1] or 0
        review_count  = row[2] or 0
        avg_cycle     = f"{row[3]:.1f}" if row[3] is not None else "n/a"
        coverage_pct  = f"{row[4]:.1f}%" if row[4] is not None else "n/a"
        print(
            f"  {repo_name:<38}  {pr_count:>5}  {review_count:>8}  "
            f"{avg_cycle:>13}  {coverage_pct:>15}"
        )

    print()


if __name__ == "__main__":
    main()
