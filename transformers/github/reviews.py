"""
transformers/github/reviews.py

Polars-based transformation for GitHub pull request review data.

Responsibilities:
- Parse submitted_at to Datetime
- Join with the cleaned PR DataFrame to compute response_time_hours
- Flag the earliest review per PR as is_first_review
- Add date_key from submitted_at
- Derive comments_count from body_length
- Deduplicate on github_review_id
- Return a clean DataFrame ready for Pandera validation and warehouse load
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

_GH_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"

_KEEP = [
    "github_review_id",
    "pr_number",
    "repo_full_name",
    "reviewer_login",
    "reviewer_id",
    "review_state",
    "submitted_at",
    "response_time_hours",
    "comments_count",
    "is_first_review",
    "date_key",
]


def transform_reviews(
    raw_reviews: list[dict[str, Any]],
    pr_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Transform raw GitHub review extractor output into a clean warehouse-ready DataFrame.

    Steps:
    1. Build a DataFrame from raw dicts
    2. Parse submitted_at → Datetime[us, UTC]
    3. Join with pr_df on (repo_full_name, pr_number) to get PR created_at
    4. Compute response_time_hours = (submitted_at - pr_created_at) in hours
    5. Flag is_first_review = True for the earliest review per (repo_full_name, pr_number)
    6. Derive comments_count from body_length (1 if body_length > 0, else 0)
    7. Add date_key as YYYYMMDD integer from submitted_at
    8. Rename state → review_state
    9. Deduplicate on github_review_id
    10. Select only warehouse columns

    Args:
        raw_reviews: List of review dicts from connectors.github.reviews.
        pr_df: Cleaned PR DataFrame from transform_pull_requests, used to resolve
               PR created_at timestamps for response time calculation.

    Returns:
        Clean Polars DataFrame with columns matching fact_reviews structure.
        Returns an empty DataFrame with the correct schema if input is empty.
    """
    logger.info("Transforming %d raw review records", len(raw_reviews))

    if not raw_reviews:
        logger.warning("No review records to transform — returning empty DataFrame")
        return _empty_review_dataframe()

    df = pl.DataFrame(raw_reviews)
    logger.debug("Raw reviews DataFrame shape: %s", df.shape)

    # --- 1. Parse timestamp ---
    df = df.with_columns(
        pl.col("submitted_at")
        .str.to_datetime(_GH_TS_FMT, strict=False, time_unit="us")
        .dt.replace_time_zone("UTC")
        .alias("submitted_at")
    )

    # --- 2. Ensure integer columns are Int64 ---
    for col in ("github_review_id", "reviewer_id", "pr_number", "body_length"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

    # --- 3. Rename state → review_state before any joins to avoid collision ---
    if "state" in df.columns:
        df = df.rename({"state": "review_state"})

    # --- 4. Join with PR data to get pr_created_at ---
    df = _join_pr_created_at(df, pr_df)

    # --- 5. Compute response_time_hours ---
    df = _compute_response_time(df)

    # --- 6. Flag is_first_review ---
    df = _add_is_first_review(df)

    # --- 7. Derive comments_count from body_length ---
    df = df.with_columns(
        pl.when(pl.col("body_length") > 0)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .cast(pl.Int64)
        .alias("comments_count")
    )

    # --- 8. Add date_key from submitted_at ---
    df = df.with_columns(
        pl.col("submitted_at")
        .dt.strftime("%Y%m%d")
        .cast(pl.Int32)
        .alias("date_key")
    )

    # --- 9. Deduplicate on natural key ---
    before = df.height
    df = df.unique(subset=["github_review_id"], keep="first", maintain_order=True)
    dropped = before - df.height
    if dropped:
        logger.warning("Dropped %d duplicate review records", dropped)

    # --- 10. Select warehouse columns ---
    available = [c for c in _KEEP if c in df.columns]
    df = df.select(available)

    logger.info("Review transformation complete: %d rows, %d columns", df.height, df.width)
    return df


def flag_reviewed_prs(pr_df: pl.DataFrame, review_df: pl.DataFrame) -> pl.DataFrame:
    """
    Update is_reviewed on the PR DataFrame based on the transformed reviews.

    A PR is considered reviewed if it has at least one review in review_df.
    This is called after both transformations are complete to back-fill the
    is_reviewed flag that defaults to False in transform_pull_requests.

    Args:
        pr_df: Cleaned PR DataFrame from transform_pull_requests.
        review_df: Cleaned reviews DataFrame from transform_reviews.

    Returns:
        pr_df with is_reviewed set to True for PRs that have reviews.
    """
    if review_df.is_empty():
        return pr_df

    reviewed_keys = (
        review_df
        .select(["repo_full_name", "pr_number"])
        .unique()
        .with_columns(pl.lit(True).alias("_has_review"))
    )

    updated = (
        pr_df
        .join(reviewed_keys, on=["repo_full_name", "pr_number"], how="left")
        .with_columns(
            pl.col("_has_review").fill_null(False).alias("is_reviewed")
        )
        .drop("_has_review")
    )

    reviewed_count = updated.filter(pl.col("is_reviewed")).height
    logger.info("Flagged %d / %d PRs as reviewed", reviewed_count, updated.height)
    return updated


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _join_pr_created_at(df: pl.DataFrame, pr_df: pl.DataFrame) -> pl.DataFrame:
    """
    Left-join review records with PR created_at timestamps.

    Falls back gracefully if pr_df is empty or missing expected columns.
    """
    required = {"repo_full_name", "pr_number", "created_at"}
    if pr_df.is_empty() or not required.issubset(set(pr_df.columns)):
        logger.warning(
            "pr_df missing required columns %s — response_time_hours will be null",
            required - set(pr_df.columns),
        )
        return df.with_columns(
            pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("pr_created_at")
        )

    pr_lookup = pr_df.select(["repo_full_name", "pr_number", "created_at"]).rename(
        {"created_at": "pr_created_at"}
    )

    return df.join(pr_lookup, on=["repo_full_name", "pr_number"], how="left")


def _compute_response_time(df: pl.DataFrame) -> pl.DataFrame:
    """Add response_time_hours: (submitted_at - pr_created_at) in hours."""
    if "pr_created_at" not in df.columns:
        return df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("response_time_hours")
        )

    return df.with_columns(
        pl.when(pl.col("pr_created_at").is_not_null())
        .then(
            (pl.col("submitted_at") - pl.col("pr_created_at"))
            .dt.total_seconds()
            .cast(pl.Float64)
            / 3600.0
        )
        .otherwise(None)
        .alias("response_time_hours")
    ).drop("pr_created_at")


def _add_is_first_review(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add is_first_review: True for the earliest submitted_at per (repo_full_name, pr_number).

    Uses a window rank — ties are broken by row order (ordinal rank).
    """
    return df.with_columns(
        (
            pl.col("submitted_at")
            .rank("ordinal")
            .over(["repo_full_name", "pr_number"])
            == 1
        ).alias("is_first_review")
    )


def _empty_review_dataframe() -> pl.DataFrame:
    """Return a zero-row DataFrame with the expected output schema."""
    return pl.DataFrame({
        "github_review_id":   pl.Series([], dtype=pl.Int64),
        "pr_number":          pl.Series([], dtype=pl.Int64),
        "repo_full_name":     pl.Series([], dtype=pl.Utf8),
        "reviewer_login":     pl.Series([], dtype=pl.Utf8),
        "reviewer_id":        pl.Series([], dtype=pl.Int64),
        "review_state":       pl.Series([], dtype=pl.Utf8),
        "submitted_at":       pl.Series([], dtype=pl.Datetime("us", "UTC")),
        "response_time_hours": pl.Series([], dtype=pl.Float64),
        "comments_count":     pl.Series([], dtype=pl.Int64),
        "is_first_review":    pl.Series([], dtype=pl.Boolean),
        "date_key":           pl.Series([], dtype=pl.Int32),
    })
