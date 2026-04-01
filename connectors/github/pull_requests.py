"""
connectors/github/pull_requests.py

GitHub pull request extractor.

Responsibilities:
- Paginate through all PRs for a given owner/repo via ghapi
- Enrich each PR with lines_added / lines_deleted / changed_files from the
  single-PR detail endpoint (the list endpoint omits these)
- Stop pagination early when `since` is provided and PRs are older than the cutoff
- Return flat dicts ready for Polars ingestion — no DataFrames here
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ghapi.all import pages

from connectors.github.client import GitHubClient

logger = logging.getLogger(__name__)


def extract_pull_requests(
    client: GitHubClient,
    owner: str,
    repo: str,
    state: str = "all",
    since: datetime | None = None,
) -> list[dict[str, Any]]:
    """
    Extract pull requests for a repository, enriched with diff statistics.

    Paginates through all matching PRs (100 per page) and fetches the detail
    endpoint for each one to obtain lines_added, lines_deleted, and changed_files,
    which are not included in the list response.

    If `since` is provided, pagination stops as soon as a PR's created_at is
    earlier than the cutoff (PRs are returned newest-first by the API).

    Args:
        client: Authenticated GitHubClient instance.
        owner: Repository owner (org or user login).
        repo: Repository name.
        state: PR state filter — 'open', 'closed', or 'all'.
        since: Optional earliest creation datetime (timezone-aware). PRs created
               before this timestamp are skipped and pagination stops.

    Returns:
        List of flat PR dicts. Each dict contains normalised scalar fields
        suitable for direct Polars DataFrame construction.
    """
    logger.info("Extracting PRs for %s/%s (state=%s, since=%s)", owner, repo, state, since)

    # Ensure since is timezone-aware for safe comparison
    if since is not None and since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    results: list[dict[str, Any]] = []

    # ghapi's pages() helper fetches page 1, inspects the Link header, and yields
    # subsequent pages. We pass the number of pages to fetch (0 = all pages).
    try:
        all_pages = pages(
            client._api.pulls.list,
            0,  # 0 = fetch all pages
            owner=owner,
            repo=repo,
            state=state,
            sort="created",
            direction="desc",
            per_page=100,
        )
    except Exception as exc:
        logger.error("Failed to fetch PR list for %s/%s: %s", owner, repo, exc)
        raise

    stop_early = False
    page_num = 0

    for page in all_pages:
        page_num += 1
        logger.debug("Processing page %d for %s/%s", page_num, owner, repo)

        for pr in page:
            pr_created = _parse_timestamp(pr.get("created_at"))

            # PRs are newest-first; once we pass the cutoff everything after is older
            if since is not None and pr_created is not None and pr_created < since:
                logger.debug(
                    "PR #%d created_at %s is before cutoff %s — stopping pagination",
                    pr.get("number"), pr_created, since,
                )
                stop_early = True
                break

            flat = _flatten_pr(client, owner, repo, pr)
            results.append(flat)

        if stop_early:
            break

    logger.info("Extracted %d PRs from %s/%s", len(results), owner, repo)
    return results


def _flatten_pr(
    client: GitHubClient,
    owner: str,
    repo: str,
    pr: Any,
) -> dict[str, Any]:
    """
    Build a flat dict from a list-endpoint PR object, enriched with diff stats
    fetched from the single-PR detail endpoint.

    Args:
        client: Authenticated GitHubClient instance.
        owner: Repository owner.
        repo: Repository name.
        pr: PR object returned by the pulls.list endpoint.

    Returns:
        Flat dict with all fields needed by the transformer layer.
    """
    pr_number = pr.get("number")

    # Fetch per-PR detail for stats not available on the list endpoint
    additions = None
    deletions = None
    changed_files = None
    merge_commit_sha = None
    merge_method = None

    try:
        detail = client.call(
            client._api.pulls.get,
            owner=owner,
            repo=repo,
            pull_number=pr_number,
        )
        additions = detail.get("additions")
        deletions = detail.get("deletions")
        changed_files = detail.get("changed_files")
        merge_commit_sha = detail.get("merge_commit_sha")
        # merge_method is not exposed in the REST API directly;
        # it appears on the merge event via the merge endpoint.
        # We leave it None here — the transformer can derive it if needed.
    except Exception as exc:
        logger.warning(
            "Could not fetch detail for PR #%d in %s/%s: %s",
            pr_number, owner, repo, exc,
        )

    author = pr.get("user") or {}
    base = pr.get("base") or {}
    head = pr.get("head") or {}

    return {
        # Identifiers
        "github_pr_id":     pr.get("id"),
        "repo_full_name":   f"{owner}/{repo}",
        "pr_number":        pr_number,
        # Content
        "title":            pr.get("title"),
        "state":            pr.get("state"),
        "draft":            pr.get("draft", False),
        # Author
        "author_login":     author.get("login"),
        "author_id":        author.get("id"),
        # Branch info
        "base_branch":      base.get("ref"),
        "head_branch":      head.get("ref"),
        "merge_commit_sha": merge_commit_sha,
        # Timestamps (ISO 8601 strings; cast to datetime in transformer)
        "created_at":       pr.get("created_at"),
        "updated_at":       pr.get("updated_at"),
        "merged_at":        pr.get("merged_at"),
        "closed_at":        pr.get("closed_at"),
        # Diff stats (from detail endpoint)
        "lines_added":      additions,
        "lines_deleted":    deletions,
        "changed_files":    changed_files,
        # Activity counts
        "commits":          pr.get("commits"),
        "comments":         pr.get("comments"),
        "review_comments":  pr.get("review_comments"),
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    """Parse a GitHub ISO 8601 timestamp string to a timezone-aware datetime."""
    if not value:
        return None
    try:
        # GitHub always returns UTC with 'Z' suffix
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
