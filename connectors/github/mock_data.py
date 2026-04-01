"""
connectors/github/mock_data.py

Deterministic mock data generators for the GitHub connector.

Produces dicts with the exact same structure as the real extractors so that
the transformer, validator, and Dagster asset layers can be developed and
tested without a live GitHub connection.

Usage:
    from connectors.github.mock_data import generate_mock_pull_requests, generate_mock_reviews

    prs = generate_mock_pull_requests("my-org/my-repo", count=50)
    reviews = generate_mock_reviews(prs)
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

# Fixed seed so every run produces the same data — useful for repeatable demos
_SEED = 42

_STATES = ["merged", "merged", "merged", "closed", "open"]  # weighted toward merged
_REVIEW_STATES = ["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "APPROVED", "APPROVED"]
_MERGE_METHODS = ["merge", "squash", "squash", "rebase"]
_BASE_BRANCHES = ["main", "main", "main", "develop", "release/1.0"]

_FIRST_NAMES = [
    "alice", "bob", "carol", "dave", "eve", "frank", "grace",
    "hank", "iris", "jack", "karen", "leo", "mia", "nate",
]
_LAST_NAMES = [
    "smith", "jones", "lee", "brown", "patel", "nguyen", "kim",
    "garcia", "wilson", "taylor",
]

_PR_TITLE_PREFIXES = [
    "feat:", "fix:", "chore:", "refactor:", "test:", "docs:", "perf:", "ci:",
]
_PR_TITLE_SUBJECTS = [
    "add login endpoint",
    "fix null pointer in parser",
    "update dependencies",
    "improve test coverage for auth module",
    "remove deprecated API calls",
    "refactor database connection pooling",
    "add retry logic to HTTP client",
    "update CI pipeline configuration",
    "fix race condition in scheduler",
    "add pagination to search results",
    "migrate to new logging framework",
    "fix memory leak in event handler",
    "add rate limit headers",
    "clean up unused imports",
    "implement feature flag support",
]


def generate_mock_pull_requests(
    repo_full_name: str,
    count: int = 50,
    days_back: int = 90,
    seed: int = _SEED,
    n_users: int = 8,
    max_lines_added: int = 800,
    cycle_time_range: tuple[int, int] = (4, 72),
) -> list[dict[str, Any]]:
    """
    Generate a list of realistic mock pull request dicts.

    The output structure exactly mirrors what connectors/github/pull_requests.py
    returns so that transformers and validators can be used interchangeably with
    real and mock data.

    Args:
        repo_full_name: Repository name in 'owner/repo' format.
        count: Number of PRs to generate.
        days_back: How many days into the past to spread PR creation dates.
        seed: Random seed for reproducibility.
        n_users: Number of distinct contributor accounts to generate.
        max_lines_added: Upper bound on lines added per PR (controls PR size).
        cycle_time_range: (min_hours, max_hours) for merged PR cycle times.

    Returns:
        List of PR dicts ordered newest-first (matching the real API sort order).
    """
    rng = random.Random(seed)
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(days=days_back)

    owner, repo = repo_full_name.split("/", 1)
    users = _generate_users(rng, n=n_users)

    prs: list[dict[str, Any]] = []

    for i in range(count):
        pr_number = count - i  # descending so index 0 = highest PR number
        github_pr_id = 100_000 + rng.randint(0, 99_999)

        # Spread creation times uniformly across the lookback window
        created_offset = timedelta(seconds=rng.uniform(0, days_back * 86_400))
        created_at = cutoff + created_offset

        state = rng.choice(_STATES)
        merged_at = None
        closed_at = None
        cycle_hours = None

        if state == "merged":
            cycle_hours = rng.uniform(*cycle_time_range)
            merged_at = created_at + timedelta(hours=cycle_hours)
            closed_at = merged_at
            # Don't let dates exceed now
            if merged_at > now:
                merged_at = now - timedelta(minutes=rng.randint(5, 120))
                closed_at = merged_at
        elif state == "closed":
            closed_offset = timedelta(hours=rng.uniform(1, 48))
            closed_at = created_at + closed_offset
            if closed_at > now:
                closed_at = now - timedelta(minutes=rng.randint(5, 60))

        author = rng.choice(users)
        lines_added = rng.randint(1, max_lines_added)
        lines_deleted = rng.randint(0, int(lines_added * 0.6) + 1)
        changed_files = rng.randint(1, min(20, lines_added // 10 + 1))

        title_prefix = rng.choice(_PR_TITLE_PREFIXES)
        title_subject = rng.choice(_PR_TITLE_SUBJECTS)

        prs.append({
            "github_pr_id":     github_pr_id,
            "repo_full_name":   repo_full_name,
            "pr_number":        pr_number,
            "title":            f"{title_prefix} {title_subject}",
            "state":            "closed" if state in ("merged", "closed") else "open",
            "draft":            rng.random() < 0.05,  # 5% chance of draft
            "author_login":     author["login"],
            "author_id":        author["id"],
            "base_branch":      rng.choice(_BASE_BRANCHES),
            "head_branch":      f"feature/{title_subject.replace(' ', '-')[:30]}",
            "merge_commit_sha": _random_sha(rng) if state == "merged" else None,
            "created_at":       _fmt(created_at),
            "updated_at":       _fmt(merged_at or closed_at or created_at),
            "merged_at":        _fmt(merged_at),
            "closed_at":        _fmt(closed_at),
            "lines_added":      lines_added,
            "lines_deleted":    lines_deleted,
            "changed_files":    changed_files,
            "commits":          rng.randint(1, 15),
            "comments":         rng.randint(0, 10),
            "review_comments":  rng.randint(0, 20),
        })

    # Sort newest-first to match real API order
    prs.sort(key=lambda p: p["created_at"] or "", reverse=True)
    return prs


def generate_mock_reviews(
    pull_requests: list[dict[str, Any]],
    seed: int = _SEED,
    review_coverage: float = 0.85,
) -> list[dict[str, Any]]:
    """
    Generate realistic mock review dicts for a list of pull requests.

    Each merged or closed PR gets 0–3 reviews. Open PRs get 0–2. Review
    submission times are between 1 and 48 hours after PR creation.

    The output structure exactly mirrors connectors/github/reviews.py.

    Args:
        pull_requests: List of PR dicts as returned by generate_mock_pull_requests
                       (or the real extractor).
        seed: Random seed for reproducibility.
        review_coverage: Fraction of PRs that receive at least one review (0.0–1.0).
                         The remainder of the weight is distributed across 1–3 reviews.

    Returns:
        Flat list of review dicts across all PRs, ordered by submitted_at ascending.
    """
    rng = random.Random(seed + 1)  # offset seed so reviews differ from PR data

    # Derive zero-review weight from desired coverage, then distribute remaining
    # weight proportionally across 1/2/3 review counts (8:6:3 ratio).
    zero_weight = max(0.0, min(1.0, 1.0 - review_coverage))
    remaining = 1.0 - zero_weight
    _review_count_weights = [zero_weight, remaining * 8 / 17, remaining * 6 / 17, remaining * 3 / 17]

    # Derive a stable set of reviewer users from the PR author set
    author_logins: list[str] = list({pr["author_login"] for pr in pull_requests if pr.get("author_login")})
    reviewer_pool = _generate_users_from_logins(author_logins)

    review_id_counter = 200_000

    reviews: list[dict[str, Any]] = []

    for pr in pull_requests:
        state = pr.get("state")
        merged = pr.get("merged_at") is not None
        created_str = pr.get("created_at")
        if not created_str:
            continue

        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))

        # Decide how many reviews this PR gets
        if merged:
            max_reviews = 3
        elif state == "closed":
            max_reviews = 2
        else:
            max_reviews = 2

        num_reviews = rng.choices(
            population=[0, 1, 2, 3],
            weights=_review_count_weights,
            k=1,
        )[0]
        num_reviews = min(num_reviews, max_reviews)

        # Pick reviewers that are not the PR author
        possible_reviewers = [
            u for u in reviewer_pool if u["login"] != pr.get("author_login")
        ]
        if not possible_reviewers:
            possible_reviewers = reviewer_pool

        # Generate reviews in chronological order
        last_offset_hours = 0.0
        for rev_idx in range(num_reviews):
            review_id_counter += rng.randint(1, 500)

            # Each successive review is a bit later than the previous
            response_hours = last_offset_hours + rng.uniform(1, 48)
            last_offset_hours = response_hours

            submitted_at = created_at + timedelta(hours=response_hours)

            # Don't let review time exceed merged_at (if merged)
            if merged and pr.get("merged_at"):
                merged_at = datetime.fromisoformat(
                    pr["merged_at"].replace("Z", "+00:00")
                )
                if submitted_at > merged_at:
                    submitted_at = merged_at - timedelta(minutes=rng.randint(5, 60))

            reviewer = rng.choice(possible_reviewers)

            # Last review on a merged PR is almost always APPROVED
            if merged and rev_idx == num_reviews - 1:
                review_state = "APPROVED"
            else:
                review_state = rng.choice(_REVIEW_STATES)

            body_length = rng.randint(0, 500) if review_state == "COMMENTED" else rng.randint(0, 120)

            reviews.append({
                "github_review_id": review_id_counter,
                "pr_number":        pr["pr_number"],
                "repo_full_name":   pr["repo_full_name"],
                "reviewer_login":   reviewer["login"],
                "reviewer_id":      reviewer["id"],
                "state":            review_state,
                "submitted_at":     _fmt(submitted_at),
                "commit_id":        _random_sha(rng),
                "html_url":         (
                    f"https://github.com/{pr['repo_full_name']}"
                    f"/pull/{pr['pr_number']}#pullrequestreview-{review_id_counter}"
                ),
                "body_length":      body_length,
            })

    reviews.sort(key=lambda r: r["submitted_at"] or "")
    return reviews


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _generate_users(rng: random.Random, n: int) -> list[dict[str, Any]]:
    """Generate n synthetic GitHub user objects."""
    used: set[str] = set()
    users: list[dict[str, Any]] = []
    while len(users) < n:
        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_NAMES)
        login = f"{first}.{last}"
        if login in used:
            login = f"{first}.{last}{rng.randint(1, 99)}"
        used.add(login)
        users.append({"login": login, "id": rng.randint(10_000, 9_999_999)})
    return users


def _generate_users_from_logins(logins: list[str]) -> list[dict[str, Any]]:
    """Reconstruct stable user dicts from a list of login strings."""
    rng = random.Random(_SEED + 99)
    return [{"login": login, "id": rng.randint(10_000, 9_999_999)} for login in logins]


def _random_sha(rng: random.Random) -> str:
    """Generate a fake 40-character hex SHA."""
    return "".join(rng.choices("0123456789abcdef", k=40))


def _fmt(dt: datetime | None) -> str | None:
    """Format a datetime as a GitHub-style ISO 8601 string, or return None."""
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
