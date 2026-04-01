"""
tests/transformers/github/test_pull_requests.py

Unit tests for transformers/github/pull_requests.py.

Test coverage targets:
- transform_pull_requests() returns a non-empty Polars DataFrame for valid input
- compute_cycle_time() is None for unmerged PRs and positive for merged PRs
- compute_weekly_merge_counts() groups correctly by ISO week
- Duplicate PR records are deduplicated on (repo_full_name, pr_number)
- Timestamps are cast to Polars Datetime dtype
"""


def test_transform_returns_dataframe():
    """Placeholder: transformer returns a Polars DataFrame for well-formed input."""
    pass


def test_cycle_time_null_for_unmerged():
    """Placeholder: cycle_time_hours is null when merged_at is null."""
    pass


def test_cycle_time_positive_for_merged():
    """Placeholder: cycle_time_hours > 0 when merged_at > created_at."""
    pass


def test_weekly_merge_counts_groups_by_week():
    """Placeholder: aggregation produces one row per (repo, ISO week)."""
    pass
