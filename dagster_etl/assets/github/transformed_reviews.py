"""
dagster_etl/assets/github/transformed_reviews.py

Dagster asset: transformed_reviews

Reads raw reviews from staging.raw_reviews, applies Polars transformations
(first review time, coverage flag), validates via Pandera, and writes
cleaned records to staging.clean_reviews.

Dependencies:
  - raw_reviews (upstream Dagster asset)
  - transformed_pull_requests (upstream — needed for PR created_at join)
  - PostgresResource: for staging table reads/writes

Downstream assets:
  - fact_reviews (warehouse layer)
"""
from dagster import asset


@asset(deps=["raw_reviews", "transformed_pull_requests"])
def transformed_reviews(postgres) -> None:
    """
    Transform raw review records and write cleaned data to staging.clean_reviews.
    """
    pass
