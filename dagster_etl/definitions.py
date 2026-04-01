"""
dagster_etl/definitions.py

Top-level Dagster Definitions object.

This is the entry point referenced in pyproject.toml under [tool.dagster].
It collects all assets, resources, jobs, and schedules from the sub-packages
and registers them with Dagster.

Usage:
    dagster dev -m dagster_etl   # starts the Dagster UI locally
"""
from dagster import Definitions


def build_definitions() -> Definitions:
    """
    Assemble and return the Dagster Definitions for the GitHub ETL pipeline.

    Returns:
        Dagster Definitions object with all assets, resources, jobs, and schedules.
    """
    pass


defs = build_definitions()
