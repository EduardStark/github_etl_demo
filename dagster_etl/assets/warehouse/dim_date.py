"""
dagster_etl/assets/warehouse/dim_date.py

Dagster asset: dim_date

Populates the dim_date calendar dimension table in PostgreSQL.
Idempotent: checks if the table is already populated before generating rows.
Generates one row per calendar day for a configured date range.

Dependencies:
  - PostgresResource
"""
from dagster import asset


@asset
def dim_date(postgres) -> None:
    """
    Populate dim_date with one row per calendar day if not already populated.
    """
    pass
