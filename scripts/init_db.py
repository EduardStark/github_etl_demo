"""
scripts/init_db.py

Database initialisation script for the GitHub ETL Demo.

Reads all SQL DDL files from the models/ directory in dependency order
(staging → dimensions → facts) and executes them against the PostgreSQL
database configured in the .env file.

Uses pg8000 (pure Python Postgres driver) instead of psycopg2 to avoid
libpq encoding issues on Windows (UnicodeDecodeError in _connect).

Usage:
    # From the project root:
    python scripts/init_db.py

    # Optionally point at a different .env:
    DATABASE_URL=postgresql://user:pass@host/db python scripts/init_db.py

Requirements:
    pip install pg8000 python-dotenv
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import pg8000.dbapi as pg
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
ENV_FILE = PROJECT_ROOT / ".env"

# SQL files executed in strict dependency order.
# staging must come first; facts depend on all dimensions.
SQL_FILES_ORDERED: list[Path] = [
    # 1. Staging schema tables (no FK dependencies)
    MODELS_DIR / "staging" / "raw_tables.sql",
    # 2. Dimension tables (no FK dependencies on each other)
    MODELS_DIR / "dimensions" / "dim_date.sql",
    MODELS_DIR / "dimensions" / "dim_repositories.sql",
    MODELS_DIR / "dimensions" / "dim_users.sql",
    # 3. Fact tables (depend on all three dimensions above)
    MODELS_DIR / "facts" / "fact_pull_requests.sql",
    MODELS_DIR / "facts" / "fact_reviews.sql",          # also depends on fact_pull_requests
    MODELS_DIR / "facts" / "fact_daily_repo_metrics.sql",
    # 4. Views (depend on facts and dimensions; CREATE OR REPLACE is idempotent)
    MODELS_DIR / "views" / "vw_pull_requests.sql",
    MODELS_DIR / "views" / "vw_reviews.sql",
    MODELS_DIR / "views" / "vw_daily_metrics.sql",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _green(text: str) -> str:
    return f"\033[92m{text}\033[0m"


def _red(text: str) -> str:
    return f"\033[91m{text}\033[0m"


def _yellow(text: str) -> str:
    return f"\033[93m{text}\033[0m"


def load_database_url() -> str:
    """Load DATABASE_URL from .env (without overriding already-set env vars)."""
    load_dotenv(ENV_FILE, encoding="utf-8")
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        print(_red("ERROR: DATABASE_URL is not set."))
        print(f"  Expected in: {ENV_FILE}")
        print("  Example: DATABASE_URL=postgresql://etl_user:password@localhost:5432/github_etl")
        sys.exit(1)
    return url


def parse_database_url(url: str) -> dict:
    """Parse a postgresql:// URL into pg8000.connect() keyword arguments."""
    parsed = urlparse(url)
    return {
        "host":     parsed.hostname or "localhost",
        "port":     parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
        "user":     parsed.username,
        "password": parsed.password,
    }


def execute_sql_file(cursor: pg.Cursor, sql_file: Path) -> None:
    """Read and execute a single SQL file. Raises on any SQL error."""
    sql = sql_file.read_text(encoding="utf-8")
    cursor.execute(sql)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("  GitHub ETL Demo — Database Initialisation")
    print("=" * 60)

    # Validate all SQL files exist before attempting a connection
    missing = [f for f in SQL_FILES_ORDERED if not f.exists()]
    if missing:
        print(_red("\nERROR: The following SQL files were not found:"))
        for f in missing:
            print(f"  {f.relative_to(PROJECT_ROOT)}")
        sys.exit(1)

    database_url = load_database_url()
    connect_kwargs = parse_database_url(database_url)

    # Mask password for display
    display_url = database_url.replace(
        f":{connect_kwargs['password']}@", ":****@"
    ) if connect_kwargs.get("password") else database_url
    print(f"\nConnecting to: {_yellow(display_url)}\n")

    try:
        conn = pg.connect(**connect_kwargs)
        conn.autocommit = False
    except Exception as exc:
        print(_red(f"ERROR: Could not connect to PostgreSQL.\n  {exc}"))
        print("\nIs the database running?  Try:  docker-compose up -d postgres")
        sys.exit(1)

    success_count = 0
    failure_count = 0

    cursor = conn.cursor()
    for sql_file in SQL_FILES_ORDERED:
        relative = sql_file.relative_to(PROJECT_ROOT)
        try:
            execute_sql_file(cursor, sql_file)
            conn.commit()
            print(_green("  OK  ") + str(relative))
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            print(_red("  FAIL") + f" {relative}")
            print(f"       {exc}")
            failure_count += 1
            # Continue attempting remaining files so all errors surface at once

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    if failure_count == 0:
        print(_green(f"  All {success_count} files executed successfully."))
        print("\n  Star schema is ready. Connect your BI tool or run:")
        print("    dagster dev -m dagster_etl")
    else:
        print(_red(f"  {failure_count} file(s) failed, {success_count} succeeded."))
        print("  Fix the errors above and re-run this script.")
        print("  All DDL uses IF NOT EXISTS so re-runs is safe.")
    print("=" * 60)

    if failure_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
