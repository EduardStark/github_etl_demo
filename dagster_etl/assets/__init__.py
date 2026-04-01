"""
dagster_etl/assets/

Software-defined assets for the GitHub ETL pipeline.
Organized into two layers:

  github/     — extraction and transformation assets (raw → clean DataFrames)
  warehouse/  — warehouse load assets (clean DataFrames → PostgreSQL star schema)
"""
