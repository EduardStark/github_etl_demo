"""
dagster_etl/assets/github/transformed_pull_requests.py

Dagster asset: transformed_pull_requests

Reads raw PRs from staging.raw_pull_requests, applies Polars transformations
(cycle time, merge flags, weekly counts), validates via Pandera, and writes
cleaned records to staging.clean_pull_requests.

Dependencies:
  - raw_pull_requests (upstream Dagster asset)
  - PostgresResource: for staging table reads/writes

Downstream assets:
  - fact_pull_requests (warehouse layer)
  - dim_repositories, dim_users (warehouse layer)
"""
from dagster import asset


@asset(deps=["raw_pull_requests"])
def transformed_pull_requests(postgres) -> None:
    """
    Transform raw PR records and write cleaned data to staging.clean_pull_requests.
    """
    pass
