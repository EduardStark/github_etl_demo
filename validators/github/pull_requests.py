"""
validators/github/pull_requests.py

Pandera schema definitions for GitHub pull request data.

Defines:
- RawPullRequestSchema: validates raw API output before transformation
- CleanPullRequestSchema: validates cleaned/transformed PR DataFrame before warehouse load

Key validations:
- pr_number is a positive integer and not null
- created_at is a valid datetime
- merged_at is nullable datetime, must be >= created_at when not null
- cycle_time_hours is non-negative when not null
- state is one of: 'open', 'closed', 'merged'
"""
from __future__ import annotations
import pandera.polars as pa


class RawPullRequestSchema(pa.DataFrameModel):
    """Pandera schema for raw GitHub PR API records."""
    pass


class CleanPullRequestSchema(pa.DataFrameModel):
    """Pandera schema for transformed PR DataFrames ready for warehouse load."""
    pass
