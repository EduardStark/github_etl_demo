# GitHub ETL Demo

A demo ETL pipeline that extracts pull request and review data from GitHub Enterprise Cloud,
transforms it with Polars, and loads it into a PostgreSQL star schema — orchestrated by Dagster.

Built as a proof-of-concept for a QA automation team's Data Science Life Cycle initiative.

---

## Quick Start

### 1. Prerequisites

- Python 3.12+
- Docker Desktop (for PostgreSQL)
- A GitHub App installed on your GitHub Enterprise Cloud organization(s)

### 2. Clone and install

```bash
git clone <repo-url>
cd GithubETLDemo
pip install -e ".[dev]"
```

### 3. Configure environment

```bash
cp .env.example .env
# Open .env and fill in all required values (see comments in the file)
```

### 4. Start PostgreSQL

```bash
docker-compose up -d postgres
```

The `docker/postgres/init.sql` script runs automatically on first start,
creating the `staging` and `public` schemas.

### 5. Start Dagster (local dev)

```bash
dagster dev -m dagster_etl
```

Open [http://localhost:3000](http://localhost:3000) in your browser to access the Dagster UI.

### 6. Run the pipeline

From the Dagster UI, navigate to **Jobs → github_pipeline** and click **Materialize all**.

---

## Project Structure

```
GithubETLDemo/
│
├── config/
│   ├── github.yaml                        # GitHub App credentials, org/repo list, API settings
│   ├── pipeline.yaml                      # Global pipeline settings (batch size, retry limits, etc.)
│   └── schemas/
│       └── repositories.yaml             # Per-repo tracking config (branches, test file patterns)
│
├── connectors/
│   ├── __init__.py
│   └── github/
│       ├── __init__.py
│       ├── client.py                      # GitHub App auth, ghapi + httpx client initialization
│       ├── pull_requests.py               # Extract PRs (created, merged, closed) via REST
│       ├── reviews.py                     # Extract PR reviews and review events via REST
│       └── graphql/
│           ├── __init__.py
│           ├── client.py                  # httpx-based GraphQL executor for GH Enterprise Cloud
│           └── queries/
│               ├── pull_requests.graphql  # GraphQL query for PR metadata with cursor pagination
│               └── reviews.graphql        # GraphQL query for review threads and authors
│
├── transformers/
│   ├── __init__.py
│   └── github/
│       ├── __init__.py
│       ├── pull_requests.py               # Polars transforms: cycle time, merge count, deduplication
│       ├── reviews.py                     # Polars transforms: first-review timestamp, reviewer dedup
│       └── dimensions.py                  # Polars logic to derive/upsert dim_users, dim_repositories
│
├── validators/
│   ├── __init__.py
│   └── github/
│       ├── __init__.py
│       ├── pull_requests.py               # Pandera schema for raw and transformed PR DataFrames
│       └── reviews.py                     # Pandera schema for review records
│
├── models/
│   ├── __init__.py
│   ├── dimensions/
│   │   ├── dim_date.sql                   # Date dimension generator (static, run once)
│   │   ├── dim_repositories.sql           # Repository dimension DDL and upsert logic
│   │   └── dim_users.sql                  # User dimension DDL and upsert logic
│   └── facts/
│       ├── fact_pull_requests.sql         # Fact table DDL: PR cycle time, merge flag, review coverage
│       ├── fact_reviews.sql               # Fact table DDL: review events, response time in minutes
│       └── fact_daily_repo_metrics.sql    # Fact table DDL: daily test file count per repo
│
├── dagster_etl/                           # Named dagster_etl to avoid conflict with dagster package
│   ├── __init__.py
│   ├── definitions.py                     # Top-level Dagster Definitions object (entry point)
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── github_resource.py             # Dagster resource wrapping GitHub client(s)
│   │   └── postgres_resource.py           # Dagster resource wrapping SQLAlchemy/psycopg2 connection
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── github/
│   │   │   ├── __init__.py
│   │   │   ├── raw_pull_requests.py       # Asset: extract raw PRs → staging layer in Postgres
│   │   │   ├── raw_reviews.py             # Asset: extract raw reviews → staging layer
│   │   │   ├── transformed_pull_requests.py   # Asset: apply transformer + validator → clean PRs
│   │   │   └── transformed_reviews.py         # Asset: apply transformer + validator → clean reviews
│   │   └── warehouse/
│   │       ├── __init__.py
│   │       ├── dim_date.py                # Asset: populate dim_date (idempotent, run-once guard)
│   │       ├── dim_repositories.py        # Asset: upsert dim_repositories from transformed data
│   │       ├── dim_users.py               # Asset: upsert dim_users from transformed data
│   │       ├── fact_pull_requests.py      # Asset: load fact_pull_requests from clean PRs + dims
│   │       ├── fact_reviews.py            # Asset: load fact_reviews from clean reviews + dims
│   │       └── fact_daily_repo_metrics.py # Asset: aggregate and load daily test file counts
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── github_pipeline.py             # Job grouping all GitHub extraction + load assets
│   └── schedules/
│       ├── __init__.py
│       └── daily_github.py                # Schedule: run github_pipeline daily at 02:00 UTC
│
├── tests/
│   ├── __init__.py
│   ├── connectors/
│   │   └── github/
│   │       ├── test_client.py             # Unit tests for auth and client init
│   │       └── test_pull_requests.py      # Unit tests for PR extraction logic
│   ├── transformers/
│   │   └── github/
│   │       ├── test_pull_requests.py      # Unit tests for cycle time and merge count transforms
│   │       └── test_reviews.py            # Unit tests for first-review-time calculation
│   └── validators/
│       └── github/
│           └── test_schemas.py            # Tests that valid/invalid DataFrames pass/fail schemas
│
├── docker/
│   ├── postgres/
│   │   └── init.sql                       # PostgreSQL init: create database, roles, staging schema
│   └── dagster/
│       └── Dockerfile                     # Dagster webserver + daemon image (configured later)
│
├── docker-compose.yml                     # Orchestrates postgres + dagster services
├── pyproject.toml                         # Project metadata and dependencies
├── .env.example                           # Template for required environment variables
├── .gitignore                             # Excludes .env, __pycache__, .dagster/, data/
└── README.md                              # This file
```

---

## KPIs Tracked

| KPI | Description | Fact Table |
|-----|-------------|------------|
| PR Cycle Time | Hours from PR creation to merge | `fact_pull_requests.cycle_time_hours` |
| PR Merge Count | Merged PRs per repo per week | `fact_pull_requests` (aggregated) |
| Review Responsiveness | Hours from PR creation to first review | `fact_reviews.time_to_first_review_hours` |
| Code Review Coverage | % of merged PRs with ≥1 review | `fact_pull_requests.had_review_before_merge` |
| Test File Trend | Daily test file count per repo | `fact_daily_repo_metrics.test_file_count` |

---

## Data Model

```
dim_date ──────────────────────────────────────────────────────────┐
dim_repositories ──────────────┬───────────────────────────────────┤
dim_users ─────────────────────┼──────────────────┐                │
                               │                  │                │
                    fact_pull_requests    fact_reviews    fact_daily_repo_metrics
```

---

## Environment Variables

See `.env.example` for the full list with descriptions. Key variables:

| Variable | Description |
|----------|-------------|
| `GITHUB_APP_ID` | GitHub App ID |
| `GITHUB_APP_PRIVATE_KEY_PATH` | Path to GitHub App `.pem` key file |
| `GITHUB_APP_INSTALLATION_ID` | GitHub App installation ID |
| `GITHUB_ENTERPRISE_URL` | GitHub Enterprise REST API base URL |
| `GITHUB_GRAPHQL_URL` | GitHub Enterprise GraphQL endpoint |
| `POSTGRES_DB` | PostgreSQL database name |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
