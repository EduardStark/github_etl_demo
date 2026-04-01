"""
scripts/test_mock_data.py

Quick sanity check for mock data generators and Polars ingestion.
Run from the project root:
    python scripts/test_mock_data.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from project root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from connectors.github.mock_data import generate_mock_pull_requests, generate_mock_reviews

# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

prs = generate_mock_pull_requests("my-org/test-repo", count=20)
reviews = generate_mock_reviews(prs)

# ---------------------------------------------------------------------------
# Counts
# ---------------------------------------------------------------------------

print(f"\n{'='*50}")
print(f"  PRs generated    : {len(prs)}")
print(f"  Reviews generated: {len(reviews)}")
print(f"{'='*50}")

# ---------------------------------------------------------------------------
# Sample PRs
# ---------------------------------------------------------------------------

SAMPLE_PR_KEYS = ("pr_number", "title", "state", "author_login", "created_at", "merged_at", "lines_added", "cycle_time_hint")

print("\n--- Sample PRs (2) ---")
for pr in prs[:2]:
    merged = pr["merged_at"]
    created = pr["created_at"]
    print(f"\n  PR #{pr['pr_number']}")
    print(f"    title      : {pr['title']}")
    print(f"    state      : {pr['state']}  merged_at={'yes' if merged else 'no'}")
    print(f"    author     : {pr['author_login']}")
    print(f"    created_at : {created}")
    print(f"    lines      : +{pr['lines_added']} / -{pr['lines_deleted']}  files={pr['changed_files']}")

# ---------------------------------------------------------------------------
# Sample reviews
# ---------------------------------------------------------------------------

print("\n--- Sample Reviews (2) ---")
for rv in reviews[:2]:
    print(f"\n  Review ID {rv['github_review_id']}")
    print(f"    PR #       : {rv['pr_number']}  repo={rv['repo_full_name']}")
    print(f"    reviewer   : {rv['reviewer_login']}")
    print(f"    state      : {rv['state']}")
    print(f"    submitted  : {rv['submitted_at']}")
    print(f"    body_length: {rv['body_length']}")

# ---------------------------------------------------------------------------
# Polars DataFrames + schemas
# ---------------------------------------------------------------------------

df_prs = pl.DataFrame(prs)
df_reviews = pl.DataFrame(reviews)

print("\n--- PR DataFrame schema ---")
for name, dtype in zip(df_prs.columns, df_prs.dtypes):
    print(f"  {name:<22} {dtype}")

print(f"\n  shape: {df_prs.shape[0]} rows × {df_prs.shape[1]} cols")

print("\n--- Reviews DataFrame schema ---")
for name, dtype in zip(df_reviews.columns, df_reviews.dtypes):
    print(f"  {name:<22} {dtype}")

print(f"\n  shape: {df_reviews.shape[0]} rows × {df_reviews.shape[1]} cols")
print()
