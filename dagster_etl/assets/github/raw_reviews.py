"""
dagster_etl/assets/github/raw_reviews.py

Dagster asset: raw_reviews

Extracts pull request review data from GitHub Enterprise Cloud for all PRs
found in staging.raw_pull_requests and loads into staging.raw_reviews.

Dependencies:
  - GitHubResource: for API access
  - PostgresResource: for staging table reads (PR numbers) and review writes

Downstream assets:
  - transformed_reviews
"""
from dagster import asset


@asset
def raw_reviews(github, postgres) -> None:
    """
    Extract raw PR reviews from GitHub and write to staging.raw_reviews.

    Reads PR numbers from staging.raw_pull_requests, fetches reviews for each
    PR via the GitHub connector, and bulk-inserts into the staging schema.
    """
    pass
