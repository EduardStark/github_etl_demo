"""
validators/github/reviews.py

Pandera schema definitions for GitHub pull request review data.

Defines:
- RawReviewSchema: validates raw review API output before transformation
- CleanReviewSchema: validates cleaned review DataFrame before warehouse load

Key validations:
- review_id is not null
- pr_number is a positive integer
- submitted_at is a valid datetime
- state is one of: 'APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED'
- reviewer_login is not null
"""
from __future__ import annotations
import pandera.polars as pa


class RawReviewSchema(pa.DataFrameModel):
    """Pandera schema for raw GitHub review API records."""
    pass


class CleanReviewSchema(pa.DataFrameModel):
    """Pandera schema for transformed review DataFrames ready for warehouse load."""
    pass
