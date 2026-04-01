"""
connectors/github/graphql/client.py

httpx-based GraphQL client for GitHub Enterprise Cloud.

Responsibilities:
- Load and execute .graphql query files from the queries/ directory
- Handle cursor-based pagination (pageInfo.endCursor / hasNextPage)
- Raise structured errors on GraphQL error responses
- Return raw response data as Python dicts
"""
from __future__ import annotations
from typing import Any
from pathlib import Path


def execute_query(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Execute a GraphQL query against the GitHub Enterprise endpoint.

    Args:
        query: GraphQL query string.
        variables: Optional dict of query variables.

    Returns:
        Parsed JSON response data dict (the 'data' key of the response).

    Raises:
        GraphQLError: If the response contains errors.
    """
    pass


def load_query(query_file: str) -> str:
    """
    Load a GraphQL query from the queries/ directory.

    Args:
        query_file: Filename of the .graphql file (e.g., 'pull_requests.graphql').

    Returns:
        Query string ready to pass to execute_query().
    """
    pass


def paginate_query(query: str, variables: dict[str, Any], data_path: list[str]) -> list[dict[str, Any]]:
    """
    Execute a paginated GraphQL query, following cursors until all pages are fetched.

    Args:
        query: GraphQL query string (must include $after cursor variable and pageInfo).
        variables: Initial query variables.
        data_path: List of keys to traverse to reach the node list in the response.

    Returns:
        Flat list of all nodes across all pages.
    """
    pass
