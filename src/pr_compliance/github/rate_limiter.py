"""Thread-safe tracker for GitHub's primary rate limit (``X-RateLimit-*`` headers).

The headers describe the *token's* shared quota, so every thread and every mapped
Airflow task sees the same real remaining count; no cross-process coordination needed.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping

from pr_compliance.exceptions import GitHubRateLimitError
from pr_compliance.metrics import RunMetrics

logger = logging.getLogger(__name__)


class RateLimiter:
    """Sleep until the reset time when ``remaining`` drops below ``floor``."""

    def __init__(
        self,
        floor: int = 50,
        max_sleep_s: float = 3900.0,
        reset_buffer_s: float = 2.0,
        metrics: RunMetrics | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.floor = floor
        self.max_sleep_s = max_sleep_s
        self.reset_buffer_s = reset_buffer_s
        self.metrics = metrics
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self.remaining: int | None = None
        self.reset_at: float | None = None

    def update(self, headers: Mapping[str, str]) -> None:
        """Record the latest quota from a response's headers (missing headers ignored)."""
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None:
            return
        try:
            rem, rst = int(remaining), float(reset)
        except ValueError:
            return
        with self._lock:
            # Responses can arrive out of order across threads; within one reset window
            # the lowest remaining value is the most recent truth.
            if self.reset_at is None or self.remaining is None or rst > self.reset_at:
                self.remaining, self.reset_at = rem, rst  # first value or a new window
            elif rst == self.reset_at:
                self.remaining = min(self.remaining, rem)
            # rst < reset_at: a stale response from the previous window; ignore it.
        if self.metrics:
            self.metrics.observe_remaining(rem)

    def mark_exhausted(self, reset_at: float | None) -> None:
        """Called on a 403/429 with ``remaining == 0``."""
        with self._lock:
            self.remaining = 0
            if reset_at is not None:
                self.reset_at = reset_at

    def seconds_until_reset(self) -> float:
        with self._lock:
            if self.remaining is None or self.reset_at is None or self.remaining >= self.floor:
                return 0.0
            return max(0.0, self.reset_at - self._clock() + self.reset_buffer_s)

    def wait_if_needed(self) -> float:
        """Block until the quota resets if we are below the floor. Returns seconds slept."""
        sleep_s = self.seconds_until_reset()
        if sleep_s <= 0:
            return 0.0
        if sleep_s > self.max_sleep_s:
            raise GitHubRateLimitError(
                f"rate limit resets in {sleep_s:.0f}s, more than max {self.max_sleep_s:.0f}s"
            )
        logger.warning(
            "rate_limit_wait",
            extra={"remaining": self.remaining, "sleep_s": round(sleep_s, 1)},
        )
        self._sleep(sleep_s)
        if self.metrics:
            self.metrics.incr("rate_limit_sleep_s", sleep_s)
        with self._lock:
            # Assume the window rolled over; the next response sets the real value.
            if self.reset_at is not None and self._clock() >= self.reset_at:
                self.remaining = None
        return sleep_s
