"""
tests/validators/github/test_schemas.py

Unit tests for Pandera schemas in validators/github/.

Test coverage targets:
- RawPullRequestSchema passes for valid raw API fixtures
- RawPullRequestSchema raises SchemaError for records with null pr_number
- CleanPullRequestSchema raises SchemaError when cycle_time_hours is negative
- CleanReviewSchema raises SchemaError for an invalid review state value
- Schemas correctly handle nullable columns (merged_at, closed_at)
"""


def test_raw_pr_schema_passes_valid_fixture():
    """Placeholder: valid raw PR fixture passes RawPullRequestSchema."""
    pass


def test_raw_pr_schema_fails_on_null_pr_number():
    """Placeholder: record with null pr_number raises Pandera SchemaError."""
    pass


def test_clean_pr_schema_fails_on_negative_cycle_time():
    """Placeholder: negative cycle_time_hours raises Pandera SchemaError."""
    pass


def test_clean_review_schema_fails_on_invalid_state():
    """Placeholder: review with state='INVALID' raises Pandera SchemaError."""
    pass
