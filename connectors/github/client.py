"""
connectors/github/client.py

GitHub REST API client backed by a Personal Access Token (PAT).

Responsibilities:
- Authenticate via GITHUB_TOKEN environment variable (or explicit token arg)
- Wrap ghapi's GhApi with rate-limit awareness and automatic retry
- Expose connectivity helpers used by extraction functions
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

from ghapi.all import GhApi

logger = logging.getLogger(__name__)


@dataclass
class RateLimitInfo:
    remaining: int
    limit: int
    reset_at: int  # Unix timestamp


@dataclass
class GitHubClient:
    """
    Thin wrapper around GhApi providing rate-limit handling and retry logic.

    Attributes:
        token: GitHub Personal Access Token. Falls back to GITHUB_TOKEN env var.
        base_url: GitHub API base URL. Defaults to https://api.github.com.
    """

    token: str = field(default="")
    base_url: str = field(default="https://api.github.com")

    # Internal ghapi instance — populated in __post_init__
    _api: GhApi = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.token:
            self.token = os.environ.get("GITHUB_TOKEN", "")
        if not self.token:
            raise ValueError(
                "GitHub token is required. Set GITHUB_TOKEN in your environment or .env file."
            )

        # ghapi accepts gh_host for GitHub Enterprise; strip trailing slash for safety
        host = self.base_url.rstrip("/")
        if host == "https://api.github.com":
            self._api = GhApi(token=self.token)
        else:
            # For GitHub Enterprise the host is the root domain, e.g. https://github.example.com
            # ghapi will append /api/v3 automatically when gh_host is set
            self._api = GhApi(token=self.token, gh_host=host)

        logger.debug("GitHubClient initialised (base_url=%s)", self.base_url)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_authenticated_user(self) -> str:
        """
        Verify connectivity and return the authenticated user's login.

        Returns:
            GitHub login name of the token owner.

        Raises:
            Exception: If the token is invalid or the API is unreachable.
        """
        logger.debug("Fetching authenticated user")
        user = self._call(self._api.users.get_authenticated)
        login: str = user["login"]
        logger.debug("Authenticated as: %s", login)
        return login

    def get_rate_limit(self) -> RateLimitInfo:
        """
        Return current REST API rate-limit status.

        Returns:
            RateLimitInfo with remaining, limit, and reset timestamp.
        """
        logger.debug("Fetching rate limit")
        data = self._call(self._api.rate_limit.get)
        core = data["resources"]["core"]
        return RateLimitInfo(
            remaining=core["remaining"],
            limit=core["limit"],
            reset_at=core["reset"],
        )

    def call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Call any ghapi function with automatic rate-limit retry.

        Args:
            func: A ghapi endpoint callable (e.g. client._api.pulls.list).
            *args: Positional args forwarded to func.
            **kwargs: Keyword args forwarded to func.

        Returns:
            The API response object.
        """
        return self._call(func, *args, **kwargs)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute a ghapi call, sleeping and retrying once on rate-limit (HTTP 403/429)."""
        logger.debug("API call: %s args=%s kwargs=%s", getattr(func, "__name__", func), args, kwargs)
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            # ghapi raises HTTP errors as plain Exceptions with the status in the message
            msg = str(exc)
            if "403" in msg or "429" in msg or "rate limit" in msg.lower():
                logger.warning("Rate limit hit. Fetching reset time and waiting…")
                self._wait_for_rate_limit_reset()
                logger.info("Retrying after rate limit wait…")
                return func(*args, **kwargs)
            raise

    def _wait_for_rate_limit_reset(self) -> None:
        """Sleep until the API rate limit resets, plus a 5-second safety buffer."""
        try:
            info = self.get_rate_limit()
            wait_seconds = max(0, info.reset_at - int(time.time())) + 5
        except Exception:
            wait_seconds = 60  # safe fallback if we can't even fetch rate limit
        logger.warning("Sleeping %d seconds until rate limit resets…", wait_seconds)
        time.sleep(wait_seconds)
