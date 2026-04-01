"""
dagster_etl/assets/warehouse/dim_repositories.py

Dagster asset: dim_repositories

Upserts repository dimension records into dim_repositories from
cleaned PR data in staging.clean_pull_requests.

Dependencies:
  - transformed_pull_requests (upstream Dagster asset)
  - PostgresResource
"""
from dagster import asset


@asset(deps=["transformed_pull_requests"])
def dim_repositories(postgres) -> None:
    """
    Upsert repository records into dim_repositories from staging data.
    """
    pass
