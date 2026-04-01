"""
scripts/test_transformers.py

Sanity check for the transformer layer.
Generates mock data, runs all transformers, prints schemas, sample rows, and KPI summary.

Run from the project root:
    python scripts/test_transformers.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from connectors.github.mock_data import generate_mock_pull_requests, generate_mock_reviews
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


def print_df(df: pl.DataFrame, name: str) -> None:
    print(f"\n--- {name} ---")
    print(f"  shape   : {df.height} rows x {df.width} cols")
    print(f"  columns :")
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"    {col:<28} {dtype}")
    print(f"\n  head(3) :")
    for i, row in enumerate(df.head(3).iter_rows(named=True)):
        # Truncate long string values for readability
        display = {k: (str(v)[:40] + "…" if isinstance(v, str) and len(str(v)) > 40 else v)
                   for k, v in row.items()}
        print(f"    [{i}] {display}")


# ---------------------------------------------------------------------------
# Generate mock data
# ---------------------------------------------------------------------------

section("1. Generating mock data")

raw_prs     = generate_mock_pull_requests("my-org/test-repo", count=30)
raw_reviews = generate_mock_reviews(raw_prs)

print(f"  Raw PRs     : {len(raw_prs)}")
print(f"  Raw reviews : {len(raw_reviews)}")

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

section("2. Running transformers")

pr_df     = transform_pull_requests(raw_prs)
review_df = transform_reviews(raw_reviews, pr_df)
pr_df     = flag_reviewed_prs(pr_df, review_df)
repo_df   = extract_dim_repositories(raw_prs)
user_df   = extract_dim_users(raw_prs, raw_reviews)

print("  All transformers completed successfully.")

# ---------------------------------------------------------------------------
# DataFrames: schema + sample rows
# ---------------------------------------------------------------------------

section("3. DataFrame schemas and sample rows")

print_df(pr_df,     "fact_pull_requests (transformed)")
print_df(review_df, "fact_reviews (transformed)")
print_df(repo_df,   "dim_repositories")
print_df(user_df,   "dim_users")

# ---------------------------------------------------------------------------
# KPI summary
# ---------------------------------------------------------------------------

section("4. KPI summary")

# --- Average cycle time for merged PRs ---
merged = pr_df.filter(pl.col("merged_at").is_not_null())
if merged.height > 0:
    avg_cycle = merged["cycle_time_hours"].drop_nulls().mean()
    print(f"\n  Merged PRs                 : {merged.height} / {pr_df.height}")
    print(f"  Avg cycle_time_hours       : {avg_cycle:.2f} h  ({avg_cycle / 24:.1f} days)")
else:
    print("\n  No merged PRs in sample.")

# --- PRs with at least one review ---
reviewed_count = pr_df.filter(pl.col("is_reviewed")).height
print(f"\n  PRs with review (is_reviewed=True) : {reviewed_count} / {pr_df.height}"
      f"  ({100 * reviewed_count / pr_df.height:.0f}%)")

# --- Avg response time for first reviews only ---
first_reviews = review_df.filter(pl.col("is_first_review"))
if first_reviews.height > 0:
    avg_response = first_reviews["response_time_hours"].drop_nulls().mean()
    print(f"\n  First reviews              : {first_reviews.height}")
    print(f"  Avg response_time_hours    : {avg_response:.2f} h  ({avg_response / 24:.1f} days)")
else:
    print("\n  No first-review data available.")

print()
