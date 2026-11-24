"""Typed shapes shared by the extractor and the transform."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, TypedDict

# Bronze entity file names. Each file is ``{"<pr_number>": <raw API payload>}`` except
# pull_requests.json which is the raw list of PR objects as returned by the list endpoint.
PULL_REQUESTS = "pull_requests"
REVIEWS = "reviews"
COMMITS = "commits"
STATUSES = "statuses"
CHECK_RUNS = "check_runs"
BRONZE_ENTITIES = (PULL_REQUESTS, REVIEWS, COMMITS, STATUSES, CHECK_RUNS)
MANIFEST = "_manifest.json"

# GitHub caps GET /pulls/{n}/commits at 250 commits.
COMMITS_LIST_CAP = 250


@dataclass(frozen=True)
class Window:
    """Half-open merge window ``[start, end)``; ``ds`` is the partition key."""

    start: datetime
    end: datetime
    ds: date

    def contains(self, ts: datetime) -> bool:
        return self.start <= ts < self.end


class SkippedPR(TypedDict):
    number: int
    resource: str
    status: int | None
    reason: str


class Manifest(TypedDict):
    repo: str
    organization: str
    ds: str
    run_id: str
    status: str  # "success" | "skipped_empty_repo"
    window_start: str
    window_end: str
    pr_count: int
    skipped_prs: list[SkippedPR]
    commits_truncated: list[int]
    max_prs_hit: bool
    api_calls: int
    duration_s: float
    extracted_at: str


RawRecord = dict[str, Any]
