"""
dagster_etl/assets/warehouse/fact_pull_requests.py

Dagster asset: fact_pull_requests

Joins cleaned PR staging data with dimension table keys and loads
rows into fact_pull_requests in the PostgreSQL warehouse.

Dependencies:
  - transformed_pull_requests
  - dim_repositories
  - dim_users
  - dim_date
  - PostgresResource
"""
from dagster import asset


@asset(deps=["transformed_pull_requests", "dim_repositories", "dim_users", "dim_date"])
def fact_pull_requests(postgres) -> None:
    """
    Load fact_pull_requests from cleaned staging data joined to dimension keys.
    """
    pass
