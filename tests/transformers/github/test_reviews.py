"""
tests/transformers/github/test_reviews.py

Unit tests for transformers/github/reviews.py.

Test coverage targets:
- transform_reviews() returns a Polars DataFrame for valid input
- compute_first_review_time() returns the minimum time delta per PR
- compute_review_coverage() correctly flags PRs with and without reviews
- PRs with only bot reviews are handled correctly
"""


def test_transform_reviews_returns_dataframe():
    """Placeholder: transformer returns a Polars DataFrame for well-formed input."""
    pass


def test_first_review_time_is_minimum():
    """Placeholder: time_to_first_review is the earliest review, not the last."""
    pass


def test_review_coverage_pct_range():
    """Placeholder: review_coverage_pct is between 0.0 and 1.0 inclusive."""
    pass
