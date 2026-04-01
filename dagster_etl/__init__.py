"""
dagster_etl/

Main Dagster package for the GitHub ETL pipeline.
Named dagster_etl (not dagster) to avoid conflicts with the dagster library.

Contains:
- definitions.py: top-level Dagster Definitions entry point
- resources/: Dagster resource wrappers (GitHub client, PostgreSQL connection)
- assets/: Dagster software-defined assets organized by layer and source
- jobs/: Job definitions grouping related assets
- schedules/: Cron-based schedule definitions
"""
