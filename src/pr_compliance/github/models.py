"""Typed shapes shared by the extractor and the transform."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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


def to_utc(value: datetime) -> datetime:
    """Plain stdlib UTC datetime from any aware datetime (including pendulum's).

    Airflow hands tasks ``pendulum.DateTime`` objects. After ``astimezone(UTC)`` they
    stay pendulum objects, and subtracting a ``timedelta`` from one returns a *naive*
    datetime, which then fails to compare with GitHub's aware timestamps.
    """
    return datetime.fromtimestamp(value.timestamp(), tz=UTC)


@dataclass(frozen=True)
class Window:
    """Half-open merge window ``[start, end)``; ``ds`` is the partition key."""

    start: datetime
    end: datetime
    ds: date

    def contains(self, ts: datetime) -> bool:
        return self.start <= ts < self.end

    @classmethod
    def for_interval(
        cls, interval_start: datetime, interval_end: datetime, lookback_days: int | None = None
    ) -> Window:
        """Merge window for a run. ``ds`` is the day the interval starts (Airflow's ``ds``).

        ``LOOKBACK_DAYS`` widens the start for demos/backfills; the partition key stays
        the same so reruns remain idempotent.
        """
        start, end = to_utc(interval_start), to_utc(interval_end)
        if lookback_days:
            start = end - timedelta(days=lookback_days)
        return cls(start=start, end=end, ds=to_utc(interval_start).date())

    @classmethod
    def previous_day(cls, run_after: datetime, lookback_days: int | None = None) -> Window:
        """The last full UTC day before ``run_after``.

        Used for manual triggers: in Airflow 3 they have no logical date and no data
        interval, so we process the same window a scheduled run at that time would.
        """
        end = to_utc(run_after).replace(hour=0, minute=0, second=0, microsecond=0)
        return cls.for_interval(end - timedelta(days=1), end, lookback_days)


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
