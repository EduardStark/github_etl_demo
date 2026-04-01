"""
models/

SQL DDL files for the star schema in PostgreSQL.
Organized into dimensions/ and facts/ sub-directories.
These files are executed by the warehouse Dagster assets during initialization
and are idempotent (CREATE TABLE IF NOT EXISTS + upsert patterns).
"""
