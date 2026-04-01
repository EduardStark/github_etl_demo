"""
dagster_etl/assets/warehouse/fact_daily_repo_metrics.py

Dagster asset: fact_daily_repo_metrics

Takes a daily snapshot of test file counts per repository and loads
into fact_daily_repo_metrics. Counts files matching configured test
patterns in each repo's default branch at pipeline run time.

Dependencies:
  - GitHubResource (for listing repo file trees)
  - dim_repositories
  - dim_date
  - PostgresResource
"""
from dagster import asset


@asset(deps=["dim_repositories", "dim_date"])
def fact_daily_repo_metrics(github, postgres) -> None:
    """
    Snapshot test file counts for each repo today and load into fact_daily_repo_metrics.
    """
    pass
