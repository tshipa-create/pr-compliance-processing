from __future__ import annotations

import pytest

from pr_compliance.exceptions import GitHubRateLimitError
from pr_compliance.github.rate_limiter import RateLimiter
from pr_compliance.metrics import RunMetrics


def make(now: float = 1000.0, **kw: object) -> tuple[RateLimiter, list[float]]:
    slept: list[float] = []
    limiter = RateLimiter(clock=lambda: now, sleep=slept.append, **kw)  # type: ignore[arg-type]
    return limiter, slept


def test_no_wait_above_floor() -> None:
    limiter, slept = make(floor=50)
    limiter.update({"X-RateLimit-Remaining": "51", "X-RateLimit-Reset": "1300"})
    assert limiter.wait_if_needed() == 0
    assert slept == []


def test_sleeps_until_reset_below_floor() -> None:
    metrics = RunMetrics()
    limiter, slept = make(floor=50, reset_buffer_s=2, metrics=metrics)
    limiter.update({"X-RateLimit-Remaining": "10", "X-RateLimit-Reset": "1300"})
    assert limiter.wait_if_needed() == pytest.approx(302)
    assert slept == [pytest.approx(302)]
    assert metrics.min_rate_limit_remaining == 10
    assert metrics.rate_limit_sleep_s == pytest.approx(302)


def test_raises_when_reset_too_far() -> None:
    limiter, _ = make(floor=50, max_sleep_s=60)
    limiter.update({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "5000"})
    with pytest.raises(GitHubRateLimitError):
        limiter.wait_if_needed()


def test_missing_or_bad_headers_ignored() -> None:
    limiter, _ = make()
    limiter.update({})
    limiter.update({"X-RateLimit-Remaining": "abc", "X-RateLimit-Reset": "1"})
    assert limiter.remaining is None


def test_out_of_order_responses_keep_lowest_in_window() -> None:
    limiter, _ = make()
    limiter.update({"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": "1300"})
    limiter.update({"X-RateLimit-Remaining": "120", "X-RateLimit-Reset": "1300"})  # late reply
    assert limiter.remaining == 100
    limiter.update({"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": "4900"})  # new window
    assert limiter.remaining == 4999
    limiter.update({"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": "1300"})  # stale window
    assert limiter.remaining == 4999
