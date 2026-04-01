"""
dagster_etl/jobs/github_pipeline.py

Dagster job: github_pipeline

Groups all GitHub extraction, transformation, and warehouse load assets
into a single executable job. This is the job triggered by the daily schedule
and can also be run manually from the Dagster UI.

Asset selection:
  - raw_pull_requests
  - raw_reviews
  - transformed_pull_requests
  - transformed_reviews
  - dim_date
  - dim_repositories
  - dim_users
  - fact_pull_requests
  - fact_reviews
  - fact_daily_repo_metrics
"""
from dagster import define_asset_job

github_pipeline_job = define_asset_job(
    name="github_pipeline",
    description="Full GitHub ETL pipeline: extract → transform → load star schema.",
)
