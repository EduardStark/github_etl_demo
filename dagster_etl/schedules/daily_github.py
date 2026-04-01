"""
dagster_etl/schedules/daily_github.py

Dagster schedule: daily_github_schedule

Triggers the github_pipeline job once per day at 02:00 UTC.
Designed for incremental extraction — only PRs and reviews updated
since the previous run are fetched (watermark-based extraction
to be implemented in the connector layer).
"""
from dagster import ScheduleDefinition
from dagster_etl.jobs.github_pipeline import github_pipeline_job

daily_github_schedule = ScheduleDefinition(
    job=github_pipeline_job,
    cron_schedule="0 2 * * *",
    name="daily_github_schedule",
    description="Run the full GitHub ETL pipeline daily at 02:00 UTC.",
)
