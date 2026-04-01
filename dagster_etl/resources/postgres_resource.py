"""
dagster_etl/resources/postgres_resource.py

Dagster resource wrapping the PostgreSQL database connection.

Provides:
- PostgresResource: a Dagster ConfigurableResource that exposes
  a SQLAlchemy engine and psycopg2 connection to assets.

Used by all warehouse-layer assets to read from staging tables
and write to the star schema.
"""
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """
    Dagster resource for PostgreSQL connectivity.

    Attributes:
        host: Database host.
        port: Database port (default 5432).
        database: Database name.
        username: Database user.
        password: Database password.
    """

    host: str
    port: int = 5432
    database: str
    username: str
    password: str

    def get_engine(self):
        """Return a SQLAlchemy engine for this PostgreSQL connection."""
        pass

    def get_connection(self):
        """Return a raw psycopg2 connection."""
        pass
