"""
connectors/github/reviews.py

GitHub pull request review extractor.

Responsibilities:
- Fetch all reviews for a list of PR numbers via the REST reviews endpoint
- Handle pagination (reviews endpoint supports per_page up to 100)
- Return flat dicts with the fields needed by the transformer layer
"""
from __future__ import annotations

import logging
from typing import Any

from ghapi.all import pages

from connectors.github.client import GitHubClient

logger = logging.getLogger(__name__)


def extract_reviews(
    client: GitHubClient,
    owner: str,
    repo: str,
    pr_numbers: list[int],
) -> list[dict[str, Any]]:
    """
    Extract all reviews for a list of pull requests in a repository.

    Iterates over each PR number and paginates through its reviews endpoint.
    Results from all PRs are combined into a single flat list.

    Args:
        client: Authenticated GitHubClient instance.
        owner: Repository owner (org or user login).
        repo: Repository name.
        pr_numbers: List of PR numbers to fetch reviews for.

    Returns:
        Flat list of review dicts, one entry per review event across all PRs.
    """
    logger.info(
        "Extracting reviews for %d PRs in %s/%s", len(pr_numbers), owner, repo
    )

    results: list[dict[str, Any]] = []

    for pr_number in pr_numbers:
        pr_reviews = _extract_reviews_for_pr(client, owner, repo, pr_number)
        results.extend(pr_reviews)
        logger.debug("PR #%d — %d review(s)", pr_number, len(pr_reviews))

    logger.info(
        "Extracted %d total reviews from %s/%s", len(results), owner, repo
    )
    return results


def _extract_reviews_for_pr(
    client: GitHubClient,
    owner: str,
    repo: str,
    pr_number: int,
) -> list[dict[str, Any]]:
    """
    Fetch and flatten all reviews for a single pull request.

    Args:
        client: Authenticated GitHubClient instance.
        owner: Repository owner.
        repo: Repository name.
        pr_number: The pull request number.

    Returns:
        List of flat review dicts for this PR.
    """
    try:
        all_pages = pages(
            client._api.pulls.list_reviews,
            0,  # 0 = fetch all pages
            owner=owner,
            repo=repo,
            pull_number=pr_number,
            per_page=100,
        )
    except Exception as exc:
        logger.warning(
            "Could not fetch reviews for PR #%d in %s/%s: %s",
            pr_number, owner, repo, exc,
        )
        return []

    results: list[dict[str, Any]] = []
    for page in all_pages:
        for review in page:
            results.append(_flatten_review(review, pr_number, owner, repo))

    return results


def _flatten_review(
    review: Any,
    pr_number: int,
    owner: str,
    repo: str,
) -> dict[str, Any]:
    """
    Build a flat dict from a GitHub review object.

    The review body length is used as a proxy for comment count since the
    reviews endpoint does not return per-review comment counts directly.

    Args:
        review: Review object from the pulls.list_reviews endpoint.
        pr_number: PR number this review belongs to (not present on the review object itself).
        owner: Repository owner (for traceability).
        repo: Repository name (for traceability).

    Returns:
        Flat dict with all review fields needed by the transformer layer.
    """
    reviewer = review.get("user") or {}
    body: str = review.get("body") or ""

    return {
        # Identifiers
        "github_review_id": review.get("id"),
        "pr_number":        pr_number,
        "repo_full_name":   f"{owner}/{repo}",
        # Reviewer
        "reviewer_login":   reviewer.get("login"),
        "reviewer_id":      reviewer.get("id"),
        # Review details
        "state":            review.get("state"),        # 'APPROVED', 'CHANGES_REQUESTED', etc.
        "submitted_at":     review.get("submitted_at"), # ISO 8601 string; cast in transformer
        "commit_id":        review.get("commit_id"),
        "html_url":         review.get("html_url"),
        # Body length as a proxy for inline comment richness
        "body_length":      len(body),
    }
