"""
connectors/database/loader.py

Loads transformed Polars DataFrames into the PostgreSQL star schema.

Design principles:
- SQLAlchemy Core with text() — explicit SQL, explicit upserts, no ORM magic
- pg8000 driver — pure Python, avoids libpq encoding issues on Windows
- FK resolution done in Python (one query per dimension per load call)
- Each public method wraps its work in a single transaction
- Missing FK targets (e.g. reviewer not in dim_users) are logged and skipped

Notes on github_repo_id:
  The GitHub REST PR endpoint does not return a numeric repository ID.
  When github_repo_id is null in the DataFrame, this loader derives a
  deterministic synthetic ID from the repo full_name using CRC32 so the
  NOT NULL DDL constraint is satisfied. In production, replace this with
  a real ID from the /repos/{owner}/{repo} endpoint.
"""
from __future__ import annotations

import logging
import os
import zlib
from typing import Any

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _synthetic_repo_id(full_name: str) -> int:
    """Deterministic BIGINT-safe ID derived from repo full_name via CRC32."""
    return zlib.crc32(full_name.encode("utf-8")) & 0x7FFF_FFFF


def _make_engine(database_url: str) -> Engine:
    """Build a SQLAlchemy engine using the pg8000 pure-Python driver."""
    url = database_url.strip()
    # Replace scheme so SQLAlchemy routes to pg8000 instead of psycopg2
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            url = "postgresql+pg8000://" + url[len(prefix):]
            break
    return create_engine(url, future=True)


def _rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame to a list of plain Python dicts for SQLAlchemy."""
    return df.to_dicts()


# ---------------------------------------------------------------------------
# DatabaseLoader
# ---------------------------------------------------------------------------

class DatabaseLoader:
    """
    Loads transformed DataFrames into the PostgreSQL star schema.

    Instantiate once per pipeline run; reuses the SQLAlchemy engine's
    connection pool across all load calls.

    Args:
        database_url: Full PostgreSQL connection URL.  Reads DATABASE_URL
                      from the environment if not supplied.
    """

    def __init__(self, database_url: str = "") -> None:
        if not database_url:
            load_dotenv()
            database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            raise ValueError(
                "database_url is required. Set DATABASE_URL in your .env file."
            )
        self._engine = _make_engine(database_url)
        logger.debug("DatabaseLoader ready (engine: %s)", self._engine.url)

    # ------------------------------------------------------------------
    # Dimensions
    # ------------------------------------------------------------------

    def upsert_dim_repositories(self, df: pl.DataFrame) -> int:
        """
        Upsert repository dimension records.

        Conflict target: full_name (always present).  github_repo_id is
        synthesised from full_name when null so the NOT NULL DDL constraint
        is satisfied.

        Args:
            df: DataFrame from transformers.github.dimensions.extract_dim_repositories.

        Returns:
            Number of rows inserted or updated.
        """
        if df.is_empty():
            logger.info("upsert_dim_repositories: empty DataFrame — skipping")
            return 0

        # Synthesise github_repo_id where missing; fill NOT NULL defaults
        rows: list[dict[str, Any]] = []
        for row in df.to_dicts():
            if row.get("github_repo_id") is None:
                row["github_repo_id"] = _synthetic_repo_id(row["repo_full_name"])
            if row.get("default_branch") is None:
                row["default_branch"] = "main"
            if row.get("is_private") is None:
                row["is_private"] = False
            rows.append(row)

        sql = text("""
            INSERT INTO dim_repositories
                (github_repo_id, repo_name, org_name, full_name,
                 default_branch, language, is_private, loaded_at)
            VALUES
                (:github_repo_id, :repo_name, :org_name, :repo_full_name,
                 :default_branch, :language, :is_private, NOW())
            ON CONFLICT (full_name) DO UPDATE SET
                github_repo_id = EXCLUDED.github_repo_id,
                repo_name      = EXCLUDED.repo_name,
                org_name       = EXCLUDED.org_name,
                default_branch = COALESCE(EXCLUDED.default_branch, dim_repositories.default_branch),
                language       = COALESCE(EXCLUDED.language,       dim_repositories.language),
                is_private     = EXCLUDED.is_private,
                loaded_at      = NOW()
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info("upsert_dim_repositories: %d row(s) processed", len(rows))
        return len(rows)

    def upsert_dim_users(self, df: pl.DataFrame) -> int:
        """
        Upsert user dimension records.

        Conflict target: github_user_id.  Rows with null github_user_id are
        skipped with a warning.

        Args:
            df: DataFrame from transformers.github.dimensions.extract_dim_users.

        Returns:
            Number of rows inserted or updated.
        """
        if df.is_empty():
            logger.info("upsert_dim_users: empty DataFrame — skipping")
            return 0

        rows = []
        skipped = 0
        for row in df.to_dicts():
            if row.get("github_user_id") is None:
                logger.warning("Skipping user with null github_user_id: %s", row.get("login"))
                skipped += 1
                continue
            row.setdefault("user_type", "User")
            rows.append(row)

        if not rows:
            logger.warning("upsert_dim_users: all rows skipped")
            return 0

        sql = text("""
            INSERT INTO dim_users
                (github_user_id, login, display_name, avatar_url, user_type)
            VALUES
                (:github_user_id, :login, :display_name, :avatar_url, :user_type)
            ON CONFLICT (github_user_id) DO UPDATE SET
                login        = EXCLUDED.login,
                display_name = COALESCE(EXCLUDED.display_name, dim_users.display_name),
                avatar_url   = COALESCE(EXCLUDED.avatar_url,   dim_users.avatar_url),
                user_type    = EXCLUDED.user_type,
                updated_at   = NOW()
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info("upsert_dim_users: %d row(s) processed, %d skipped", len(rows), skipped)
        return len(rows)

    # ------------------------------------------------------------------
    # Facts
    # ------------------------------------------------------------------

    def load_fact_pull_requests(self, df: pl.DataFrame) -> int:
        """
        Resolve FK surrogate keys then upsert into fact_pull_requests.

        FK resolution:
          repo_key   ← dim_repositories.repo_key  WHERE full_name = repo_full_name
          author_key ← dim_users.user_key          WHERE login = author_login
          date_key   already computed by the transformer (YYYYMMDD Int32)

        PRs whose repo_key cannot be resolved are skipped (repo not yet in dim).
        PRs whose author_key cannot be resolved get author_key = NULL (allowed by DDL).

        Args:
            df: Cleaned PR DataFrame from transformers.github.pull_requests.

        Returns:
            Number of rows inserted or updated.
        """
        if df.is_empty():
            logger.info("load_fact_pull_requests: empty DataFrame — skipping")
            return 0

        with self._engine.begin() as conn:
            repo_keys  = _fetch_repo_keys(conn)
            user_keys  = _fetch_user_keys(conn)

        rows = []
        skipped = 0
        for row in df.to_dicts():
            repo_key = repo_keys.get(row["repo_full_name"])
            if repo_key is None:
                logger.warning(
                    "load_fact_pull_requests: no repo_key for %r — skipping PR #%s",
                    row["repo_full_name"], row.get("pr_number"),
                )
                skipped += 1
                continue

            # Derive state: mark as 'merged' when merged_at is set
            state = "merged" if row.get("merged_at") else row.get("state", "open")

            rows.append({
                "github_pr_id":          row["github_pr_id"],
                "repo_key":              repo_key,
                "author_key":            user_keys.get(row.get("author_login")),  # nullable
                "date_key":              row["date_key"],
                "pr_number":             row["pr_number"],
                "title":                 row.get("title"),
                "state":                 state,
                "created_at":            row["created_at"],
                "merged_at":             row.get("merged_at"),
                "closed_at":             row.get("closed_at"),
                "lines_added":           row.get("lines_added"),
                "lines_deleted":         row.get("lines_deleted"),
                "files_changed":         row.get("files_changed"),
                "commits_count":         row.get("commits_count", 0),
                "comments_count":        row.get("comments_count", 0),
                "review_comments_count": row.get("review_comments_count", 0),
                "cycle_time_hours":      row.get("cycle_time_hours"),
                "is_reviewed":           row.get("is_reviewed", False),
                "merge_method":          row.get("merge_method"),
            })

        if not rows:
            logger.warning("load_fact_pull_requests: all rows skipped (%d)", skipped)
            return 0

        sql = text("""
            INSERT INTO fact_pull_requests (
                github_pr_id, repo_key, author_key, date_key,
                pr_number, title, state,
                created_at, merged_at, closed_at,
                lines_added, lines_deleted, files_changed,
                commits_count, comments_count, review_comments_count,
                cycle_time_hours, is_reviewed, merge_method
            ) VALUES (
                :github_pr_id, :repo_key, :author_key, :date_key,
                :pr_number, :title, :state,
                :created_at, :merged_at, :closed_at,
                :lines_added, :lines_deleted, :files_changed,
                :commits_count, :comments_count, :review_comments_count,
                :cycle_time_hours, :is_reviewed, :merge_method
            )
            ON CONFLICT (github_pr_id, repo_key) DO UPDATE SET
                state                 = EXCLUDED.state,
                merged_at             = EXCLUDED.merged_at,
                closed_at             = EXCLUDED.closed_at,
                lines_added           = EXCLUDED.lines_added,
                lines_deleted         = EXCLUDED.lines_deleted,
                files_changed         = EXCLUDED.files_changed,
                commits_count         = EXCLUDED.commits_count,
                comments_count        = EXCLUDED.comments_count,
                review_comments_count = EXCLUDED.review_comments_count,
                cycle_time_hours      = EXCLUDED.cycle_time_hours,
                is_reviewed           = EXCLUDED.is_reviewed,
                merge_method          = EXCLUDED.merge_method,
                loaded_at             = NOW()
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info(
            "load_fact_pull_requests: %d row(s) upserted, %d skipped", len(rows), skipped
        )
        return len(rows)

    def load_fact_reviews(self, df: pl.DataFrame) -> int:
        """
        Resolve FK surrogate keys then upsert into fact_reviews.

        FK resolution:
          pr_key       ← fact_pull_requests.pr_key WHERE github_pr_id = github_pr_id
                                                    AND repo_key matches via repo_full_name
          reviewer_key ← dim_users.user_key WHERE login = reviewer_login  (nullable)
          date_key     already computed by the transformer

        Reviews whose pr_key cannot be resolved (PR not yet loaded) are skipped.

        Args:
            df: Cleaned review DataFrame from transformers.github.reviews.

        Returns:
            Number of rows inserted or updated.
        """
        if df.is_empty():
            logger.info("load_fact_reviews: empty DataFrame — skipping")
            return 0

        with self._engine.begin() as conn:
            repo_keys = _fetch_repo_keys(conn)
            user_keys = _fetch_user_keys(conn)
            pr_keys   = _fetch_pr_keys(conn)   # (github_pr_id, repo_key) → pr_key

        rows = []
        skipped = 0
        for row in df.to_dicts():
            repo_key = repo_keys.get(row["repo_full_name"])
            if repo_key is None:
                logger.warning(
                    "load_fact_reviews: no repo_key for %r — skipping review %s",
                    row["repo_full_name"], row.get("github_review_id"),
                )
                skipped += 1
                continue

            pr_key = pr_keys.get((row["pr_number"], repo_key))
            if pr_key is None:
                logger.warning(
                    "load_fact_reviews: no pr_key for PR #%s in repo_key=%s — skipping",
                    row.get("pr_number"), repo_key,
                )
                skipped += 1
                continue

            rows.append({
                "github_review_id":   row["github_review_id"],
                "pr_key":             pr_key,
                "reviewer_key":       user_keys.get(row.get("reviewer_login")),  # nullable
                "date_key":           row["date_key"],
                "review_state":       row["review_state"],
                "submitted_at":       row["submitted_at"],
                "response_time_hours": row.get("response_time_hours"),
                "comments_count":     row.get("comments_count", 0),
                "is_first_review":    row.get("is_first_review", False),
            })

        if not rows:
            logger.warning("load_fact_reviews: all rows skipped (%d)", skipped)
            return 0

        sql = text("""
            INSERT INTO fact_reviews (
                github_review_id, pr_key, reviewer_key, date_key,
                review_state, submitted_at,
                response_time_hours, comments_count, is_first_review
            ) VALUES (
                :github_review_id, :pr_key, :reviewer_key, :date_key,
                :review_state, :submitted_at,
                :response_time_hours, :comments_count, :is_first_review
            )
            ON CONFLICT (github_review_id) DO UPDATE SET
                review_state        = EXCLUDED.review_state,
                response_time_hours = EXCLUDED.response_time_hours,
                comments_count      = EXCLUDED.comments_count,
                is_first_review     = EXCLUDED.is_first_review,
                loaded_at           = NOW()
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info(
            "load_fact_reviews: %d row(s) upserted, %d skipped", len(rows), skipped
        )
        return len(rows)

    def load_fact_daily_metrics(
        self,
        pr_df: pl.DataFrame,
        review_df: pl.DataFrame,
    ) -> int:
        """
        Aggregate per-repo per-day metrics from transformed DataFrames and upsert.

        Aggregation logic (grouped by repo_full_name + date_key from PR creation date):
          open_prs            — PRs with state='open' created on that day
          merged_prs          — PRs with merged_at not null created on that day
          total_commits       — sum of commits_count for PRs created on that day
          active_contributors — distinct author_login count on that day
          test_file_count     — 0 (populated by a separate file-tree extraction step)

        The date_key is the PR creation date, not a calendar snapshot date.
        This means each row represents activity on the day PRs were created.

        Args:
            pr_df:     Cleaned PR DataFrame (must have is_reviewed already set).
            review_df: Cleaned reviews DataFrame (used for future enrichment; not
                       currently aggregated here as reviews span different dates).

        Returns:
            Number of rows inserted or updated.
        """
        if pr_df.is_empty():
            logger.info("load_fact_daily_metrics: empty PR DataFrame — skipping")
            return 0

        daily = (
            pr_df
            .with_columns([
                pl.when(pl.col("merged_at").is_not_null())
                  .then(pl.lit(1)).otherwise(pl.lit(0)).alias("_is_merged"),
                pl.when(pl.col("merged_at").is_null() & pl.col("closed_at").is_null())
                  .then(pl.lit(1)).otherwise(pl.lit(0)).alias("_is_open"),
            ])
            .group_by(["repo_full_name", "date_key"])
            .agg([
                pl.col("_is_open").sum().alias("open_prs"),
                pl.col("_is_merged").sum().alias("merged_prs"),
                pl.col("commits_count").sum().alias("total_commits"),
                pl.col("author_login").n_unique().alias("active_contributors"),
            ])
            .sort(["repo_full_name", "date_key"])
        )

        with self._engine.begin() as conn:
            repo_keys = _fetch_repo_keys(conn)

        rows = []
        skipped = 0
        for row in daily.to_dicts():
            repo_key = repo_keys.get(row["repo_full_name"])
            if repo_key is None:
                logger.warning(
                    "load_fact_daily_metrics: no repo_key for %r — skipping date_key=%s",
                    row["repo_full_name"], row.get("date_key"),
                )
                skipped += 1
                continue
            rows.append({
                "repo_key":           repo_key,
                "date_key":           row["date_key"],
                "open_prs":           row["open_prs"],
                "merged_prs":         row["merged_prs"],
                "total_commits":      row["total_commits"],
                "active_contributors": row["active_contributors"],
                "test_file_count":    0,      # populated by separate file-tree extraction
                "production_file_count": 0,
                "test_coverage_ratio":   None,
            })

        if not rows:
            logger.warning("load_fact_daily_metrics: all rows skipped (%d)", skipped)
            return 0

        sql = text("""
            INSERT INTO fact_daily_repo_metrics (
                repo_key, date_key,
                open_prs, merged_prs, total_commits, active_contributors,
                test_file_count, production_file_count, test_coverage_ratio
            ) VALUES (
                :repo_key, :date_key,
                :open_prs, :merged_prs, :total_commits, :active_contributors,
                :test_file_count, :production_file_count, :test_coverage_ratio
            )
            ON CONFLICT (repo_key, date_key) DO UPDATE SET
                open_prs              = EXCLUDED.open_prs,
                merged_prs            = EXCLUDED.merged_prs,
                total_commits         = EXCLUDED.total_commits,
                active_contributors   = EXCLUDED.active_contributors,
                test_file_count       = EXCLUDED.test_file_count,
                production_file_count = EXCLUDED.production_file_count,
                test_coverage_ratio   = EXCLUDED.test_coverage_ratio,
                loaded_at             = NOW()
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info(
            "load_fact_daily_metrics: %d row(s) upserted, %d skipped", len(rows), skipped
        )
        return len(rows)


# ---------------------------------------------------------------------------
# FK resolution helpers  (package-private)
# ---------------------------------------------------------------------------

def _fetch_repo_keys(conn: Any) -> dict[str, int]:
    """Return {full_name: repo_key} for all rows in dim_repositories."""
    result = conn.execute(text("SELECT full_name, repo_key FROM dim_repositories"))
    return {row[0]: row[1] for row in result}


def _fetch_user_keys(conn: Any) -> dict[str, int]:
    """Return {login: user_key} for all rows in dim_users."""
    result = conn.execute(text("SELECT login, user_key FROM dim_users"))
    return {row[0]: row[1] for row in result}


def _fetch_pr_keys(conn: Any) -> dict[tuple[int, int], int]:
    """Return {(pr_number, repo_key): pr_key} for all rows in fact_pull_requests."""
    result = conn.execute(
        text("SELECT pr_number, repo_key, pr_key FROM fact_pull_requests")
    )
    return {(row[0], row[1]): row[2] for row in result}
