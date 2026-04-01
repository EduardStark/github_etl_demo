"""
dagster_etl/resources/github_resource.py

Dagster resource wrapping the GitHub connector clients.

Provides:
- GitHubResource: a Dagster ConfigurableResource that exposes
  authenticated REST (ghapi) and GraphQL (httpx) clients to assets.

Configuration is sourced from environment variables via Dagster's
resource config system, not directly from YAML files.
"""
from dagster import ConfigurableResource


class GitHubResource(ConfigurableResource):
    """
    Dagster resource for GitHub API access.

    Attributes:
        app_id: GitHub App ID.
        private_key_path: Path to the GitHub App private key .pem file.
        installation_id: GitHub App installation ID.
        base_url: GitHub Enterprise REST API base URL.
        graphql_url: GitHub Enterprise GraphQL endpoint URL.
    """

    app_id: str
    private_key_path: str
    installation_id: str
    base_url: str
    graphql_url: str

    def get_rest_client(self):
        """Return an authenticated ghapi REST client."""
        pass

    def get_graphql_client(self):
        """Return an authenticated httpx GraphQL client."""
        pass
