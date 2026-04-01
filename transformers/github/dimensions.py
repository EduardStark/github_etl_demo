"""
transformers/github/dimensions.py

Derives dimension table records from raw GitHub extractor output.

Responsibilities:
- Parse repo_full_name into org_name / repo_name for dim_repositories
- Collect unique users from PR authors and reviewers for dim_users
- Deduplicate and return DataFrames shaped to match the DDL schemas

Note on github_repo_id:
  The GitHub REST PR endpoints do not return a numeric repository ID.
  dim_repositories.github_repo_id is therefore left null here and must be
  populated by the warehouse asset using a separate /repos/{owner}/{repo}
  API call before inserting into the NOT NULL column.
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


def extract_dim_repositories(raw_prs: list[dict[str, Any]]) -> pl.DataFrame:
    """
    Derive unique repository dimension records from raw PR data.

    Parses repo_full_name ('org/repo') into org_name and repo_name.
    All other dim_repositories columns (language, is_private, created_at, etc.)
    are not available from PR data and are returned as null — they must be
    enriched by the warehouse asset via a /repos/{owner}/{repo} API call.

    Args:
        raw_prs: Raw PR dicts from connectors.github.pull_requests.

    Returns:
        Polars DataFrame with columns:
          repo_full_name, org_name, repo_name
          (github_repo_id, default_branch, language, is_private are null placeholders)
    """
    logger.info("Extracting repository dimensions from %d raw PR records", len(raw_prs))

    if not raw_prs:
        logger.warning("No PR records — returning empty dim_repositories DataFrame")
        return _empty_repo_dataframe()

    df = pl.DataFrame({"repo_full_name": [pr.get("repo_full_name") for pr in raw_prs]})

    df = (
        df
        .filter(pl.col("repo_full_name").is_not_null())
        .unique(subset=["repo_full_name"], maintain_order=True)
        .with_columns([
            pl.col("repo_full_name")
              .str.split("/")
              .list.get(0)
              .alias("org_name"),
            pl.col("repo_full_name")
              .str.split("/")
              .list.get(1)
              .alias("repo_name"),
            # Enriched by warehouse asset — null until /repos API call is made
            pl.lit(None).cast(pl.Int64).alias("github_repo_id"),
            pl.lit(None).cast(pl.Utf8).alias("default_branch"),
            pl.lit(None).cast(pl.Utf8).alias("language"),
            pl.lit(None).cast(pl.Boolean).alias("is_private"),
        ])
        .select([
            "github_repo_id",
            "repo_name",
            "org_name",
            "repo_full_name",
            "default_branch",
            "language",
            "is_private",
        ])
    )

    logger.info("Extracted %d unique repository dimension records", df.height)
    return df


def extract_dim_users(
    raw_prs: list[dict[str, Any]],
    raw_reviews: list[dict[str, Any]],
) -> pl.DataFrame:
    """
    Derive unique user dimension records from PR authors and reviewers.

    Collects all unique logins from:
    - PR author_login / author_id fields
    - Review reviewer_login / reviewer_id fields

    The two sets are unioned and deduplicated on login. Where the same login
    appears in both sets, the PR author record takes precedence (reviews rarely
    carry more information than PR author records).

    display_name, avatar_url, and user_type are not available from raw PR/review
    data and are returned as null placeholders.

    Args:
        raw_prs: Raw PR dicts from connectors.github.pull_requests.
        raw_reviews: Raw review dicts from connectors.github.reviews.

    Returns:
        Polars DataFrame with columns:
          github_user_id, login, display_name, avatar_url, user_type
    """
    total_inputs = len(raw_prs) + len(raw_reviews)
    logger.info(
        "Extracting user dimensions from %d PRs + %d reviews",
        len(raw_prs), len(raw_reviews),
    )

    if total_inputs == 0:
        logger.warning("No records — returning empty dim_users DataFrame")
        return _empty_user_dataframe()

    frames: list[pl.DataFrame] = []

    # --- PR authors ---
    if raw_prs:
        authors = pl.DataFrame({
            "login":          [pr.get("author_login") for pr in raw_prs],
            "github_user_id": [pr.get("author_id")    for pr in raw_prs],
        })
        frames.append(authors)

    # --- Reviewers ---
    if raw_reviews:
        reviewers = pl.DataFrame({
            "login":          [rv.get("reviewer_login") for rv in raw_reviews],
            "github_user_id": [rv.get("reviewer_id")    for rv in raw_reviews],
        })
        frames.append(reviewers)

    combined = pl.concat(frames)

    df = (
        combined
        .filter(pl.col("login").is_not_null())
        .with_columns(
            pl.col("github_user_id").cast(pl.Int64, strict=False)
        )
        # Deduplicate: keep first occurrence of each login (PR authors first)
        .unique(subset=["login"], keep="first", maintain_order=True)
        .with_columns([
            pl.lit(None).cast(pl.Utf8).alias("display_name"),
            pl.lit(None).cast(pl.Utf8).alias("avatar_url"),
            pl.lit("User").alias("user_type"),
        ])
        .select([
            "github_user_id",
            "login",
            "display_name",
            "avatar_url",
            "user_type",
        ])
    )

    logger.info("Extracted %d unique user dimension records", df.height)
    return df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _empty_repo_dataframe() -> pl.DataFrame:
    return pl.DataFrame({
        "github_repo_id":  pl.Series([], dtype=pl.Int64),
        "repo_name":       pl.Series([], dtype=pl.Utf8),
        "org_name":        pl.Series([], dtype=pl.Utf8),
        "repo_full_name":  pl.Series([], dtype=pl.Utf8),
        "default_branch":  pl.Series([], dtype=pl.Utf8),
        "language":        pl.Series([], dtype=pl.Utf8),
        "is_private":      pl.Series([], dtype=pl.Boolean),
    })


def _empty_user_dataframe() -> pl.DataFrame:
    return pl.DataFrame({
        "github_user_id": pl.Series([], dtype=pl.Int64),
        "login":          pl.Series([], dtype=pl.Utf8),
        "display_name":   pl.Series([], dtype=pl.Utf8),
        "avatar_url":     pl.Series([], dtype=pl.Utf8),
        "user_type":      pl.Series([], dtype=pl.Utf8),
    })
