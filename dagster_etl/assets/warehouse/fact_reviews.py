"""
dagster_etl/assets/warehouse/fact_reviews.py

Dagster asset: fact_reviews

Joins cleaned review staging data with dimension table keys and loads
rows into fact_reviews in the PostgreSQL warehouse.

Dependencies:
  - transformed_reviews
  - dim_repositories
  - dim_users
  - dim_date
  - fact_pull_requests (for pr_key foreign key lookup)
  - PostgresResource
"""
from dagster import asset


@asset(deps=["transformed_reviews", "dim_repositories", "dim_users", "dim_date", "fact_pull_requests"])
def fact_reviews(postgres) -> None:
    """
    Load fact_reviews from cleaned staging data joined to dimension keys.
    """
    pass
