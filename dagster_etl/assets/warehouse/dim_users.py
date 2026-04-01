"""
dagster_etl/assets/warehouse/dim_users.py

Dagster asset: dim_users

Upserts user dimension records into dim_users from PR authors and
review authors found in staging cleaned tables.

Dependencies:
  - transformed_pull_requests (upstream Dagster asset)
  - transformed_reviews (upstream Dagster asset)
  - PostgresResource
"""
from dagster import asset


@asset(deps=["transformed_pull_requests", "transformed_reviews"])
def dim_users(postgres) -> None:
    """
    Upsert user records into dim_users from staging PR and review data.
    """
    pass
