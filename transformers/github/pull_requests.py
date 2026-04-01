"""
transformers/github/pull_requests.py

Polars-based transformation for GitHub pull request data.

Responsibilities:
- Cast raw API strings to typed Polars columns (Datetime, Int64, Boolean)
- Compute derived KPI columns: cycle_time_hours, date_key
- Rename fields to match fact_pull_requests DDL
- Deduplicate on (repo_full_name, pr_number)
- Return a clean DataFrame ready for Pandera validation and warehouse load
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)

# GitHub ISO-8601 timestamp format returned by the REST API
_GH_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"

# Columns kept after transformation — maps extractor field → fact_pull_requests column.
# Any extractor field not listed here is dropped.
_RENAME: dict[str, str] = {
    "changed_files":    "files_changed",
    "commits":          "commits_count",
    "comments":         "comments_count",
    "review_comments":  "review_comments_count",
}

_KEEP_AFTER_RENAME = [
    "github_pr_id",
    "repo_full_name",
    "pr_number",
    "title",
    "state",
    "author_login",
    "author_id",
    "created_at",
    "merged_at",
    "closed_at",
    "lines_added",
    "lines_deleted",
    "files_changed",
    "commits_count",
    "comments_count",
    "review_comments_count",
    # Derived columns added below
    "cycle_time_hours",
    "is_reviewed",
    "date_key",
]


def transform_pull_requests(raw_prs: list[dict[str, Any]]) -> pl.DataFrame:
    """
    Transform raw GitHub PR extractor output into a clean warehouse-ready DataFrame.

    Steps:
    1. Build a DataFrame from raw dicts
    2. Parse timestamp strings → Polars Datetime[us, UTC]
    3. Compute cycle_time_hours and date_key
    4. Rename columns to match fact_pull_requests DDL
    5. Deduplicate on (repo_full_name, pr_number), keeping the first occurrence
    6. Select only the columns needed for the warehouse

    Args:
        raw_prs: List of PR dicts from connectors.github.pull_requests.

    Returns:
        Clean Polars DataFrame with columns matching fact_pull_requests structure.
        Returns an empty DataFrame with the correct schema if input is empty.
    """
    logger.info("Transforming %d raw PR records", len(raw_prs))

    if not raw_prs:
        logger.warning("No PR records to transform — returning empty DataFrame")
        return _empty_pr_dataframe()

    df = pl.DataFrame(raw_prs)
    logger.debug("Raw PR DataFrame shape: %s", df.shape)

    # --- 1. Parse timestamps ---
    for col in ("created_at", "updated_at", "merged_at", "closed_at"):
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .str.to_datetime(_GH_TS_FMT, strict=False, time_unit="us")
                .dt.replace_time_zone("UTC")
                .alias(col)
            )

    # --- 2. Ensure nullable integer columns are Int64 (not UInt or mixed) ---
    for col in ("github_pr_id", "author_id", "lines_added", "lines_deleted",
                "changed_files", "commits", "comments", "review_comments"):
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Int64, strict=False)
            )

    # --- 3. Derived: cycle_time_hours (null when not merged) ---
    df = _compute_cycle_time(df)

    # --- 4. Derived: date_key as YYYYMMDD integer from created_at ---
    df = _add_date_key(df, source_col="created_at")

    # --- 5. Default is_reviewed = False (updated later when reviews are joined) ---
    df = df.with_columns(pl.lit(False).alias("is_reviewed"))

    # --- 6. Rename to match DDL ---
    df = df.rename({k: v for k, v in _RENAME.items() if k in df.columns})

    # --- 7. Deduplicate on natural key ---
    before = df.height
    df = df.unique(subset=["repo_full_name", "pr_number"], keep="first", maintain_order=True)
    dropped = before - df.height
    if dropped:
        logger.warning("Dropped %d duplicate PR records", dropped)

    # --- 8. Select only warehouse columns (in defined order) ---
    available = [c for c in _KEEP_AFTER_RENAME if c in df.columns]
    df = df.select(available)

    logger.info("PR transformation complete: %d rows, %d columns", df.height, df.width)
    return df


def compute_weekly_merge_counts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate merge counts per (repo_full_name, ISO week).

    Used for the 'PR merge count per repo per week' KPI.

    Args:
        df: Cleaned PR DataFrame as returned by transform_pull_requests.

    Returns:
        Aggregated DataFrame with columns: repo_full_name, week_str, merge_count.
        week_str format: 'YYYY-WNN' (e.g. '2024-W03').
    """
    if df.is_empty():
        return pl.DataFrame({"repo_full_name": [], "week_str": [], "merge_count": []})

    merged = df.filter(pl.col("merged_at").is_not_null())

    if merged.is_empty():
        return pl.DataFrame({"repo_full_name": [], "week_str": [], "merge_count": []})

    return (
        merged
        .with_columns(
            (
                pl.col("merged_at").dt.year().cast(pl.Utf8)
                + pl.lit("-W")
                + pl.col("merged_at").dt.week().cast(pl.Utf8).str.zfill(2)
            ).alias("week_str")
        )
        .group_by(["repo_full_name", "week_str"])
        .agg(pl.len().alias("merge_count"))
        .sort(["repo_full_name", "week_str"])
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_cycle_time(df: pl.DataFrame) -> pl.DataFrame:
    """Add cycle_time_hours: (merged_at - created_at) as Float64 hours, null if not merged."""
    if "merged_at" not in df.columns or "created_at" not in df.columns:
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("cycle_time_hours"))

    return df.with_columns(
        pl.when(pl.col("merged_at").is_not_null())
        .then(
            (pl.col("merged_at") - pl.col("created_at"))
            .dt.total_seconds()
            .cast(pl.Float64)
            / 3600.0
        )
        .otherwise(None)
        .alias("cycle_time_hours")
    )


def _add_date_key(df: pl.DataFrame, source_col: str = "created_at") -> pl.DataFrame:
    """Add date_key as YYYYMMDD integer derived from a Datetime column."""
    if source_col not in df.columns:
        return df.with_columns(pl.lit(None).cast(pl.Int32).alias("date_key"))

    return df.with_columns(
        pl.col(source_col)
        .dt.strftime("%Y%m%d")
        .cast(pl.Int32)
        .alias("date_key")
    )


def _empty_pr_dataframe() -> pl.DataFrame:
    """Return a zero-row DataFrame with the expected output schema."""
    return pl.DataFrame({
        "github_pr_id":          pl.Series([], dtype=pl.Int64),
        "repo_full_name":        pl.Series([], dtype=pl.Utf8),
        "pr_number":             pl.Series([], dtype=pl.Int64),
        "title":                 pl.Series([], dtype=pl.Utf8),
        "state":                 pl.Series([], dtype=pl.Utf8),
        "author_login":          pl.Series([], dtype=pl.Utf8),
        "author_id":             pl.Series([], dtype=pl.Int64),
        "created_at":            pl.Series([], dtype=pl.Datetime("us", "UTC")),
        "merged_at":             pl.Series([], dtype=pl.Datetime("us", "UTC")),
        "closed_at":             pl.Series([], dtype=pl.Datetime("us", "UTC")),
        "lines_added":           pl.Series([], dtype=pl.Int64),
        "lines_deleted":         pl.Series([], dtype=pl.Int64),
        "files_changed":         pl.Series([], dtype=pl.Int64),
        "commits_count":         pl.Series([], dtype=pl.Int64),
        "comments_count":        pl.Series([], dtype=pl.Int64),
        "review_comments_count": pl.Series([], dtype=pl.Int64),
        "cycle_time_hours":      pl.Series([], dtype=pl.Float64),
        "is_reviewed":           pl.Series([], dtype=pl.Boolean),
        "date_key":              pl.Series([], dtype=pl.Int32),
    })
