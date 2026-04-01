"""
dagster_etl/assets/github/

Dagster assets for the GitHub data layer.

Asset execution order:
  1. raw_pull_requests    — extract PRs from GitHub API → staging.raw_pull_requests
  2. raw_reviews          — extract reviews from GitHub API → staging.raw_reviews
  3. transformed_pull_requests — clean and enrich PRs → staging.clean_pull_requests
  4. transformed_reviews       — clean and enrich reviews → staging.clean_reviews
"""
