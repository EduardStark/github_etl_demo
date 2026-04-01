"""
dagster_etl/assets/github/raw_pull_requests.py

Dagster asset: raw_pull_requests

Extracts pull request data from GitHub Enterprise Cloud for all configured
repositories and loads it into the staging.raw_pull_requests table in PostgreSQL.

Dependencies:
  - GitHubResource: for API access
  - PostgresResource: for staging table writes

Downstream assets:
  - transformed_pull_requests
"""
from dagster import asset


@asset
def raw_pull_requests(github, postgres) -> None:
    """
    Extract raw pull requests from GitHub and write to staging.raw_pull_requests.

    Iterates over all repositories in config/github.yaml, fetches PRs via the
    GitHub connector, and bulk-inserts raw records into the staging schema.
    """
    pass
