"""
dagster_etl/assets/warehouse/

Dagster assets for the warehouse (star schema) load layer.

Asset execution order:
  1. dim_date             — populate calendar dimension (idempotent, run-once)
  2. dim_repositories     — upsert repository records
  3. dim_users            — upsert user records
  4. fact_pull_requests   — load PR fact rows
  5. fact_reviews         — load review fact rows
  6. fact_daily_repo_metrics — aggregate and load daily test file snapshots
"""
