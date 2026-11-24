"""Small typed GitHub REST client: auth, timeouts, rate limits, retries, pagination."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pr_compliance.config import Settings
from pr_compliance.exceptions import (
    ConfigurationError,
    GitHubAPIError,
    GitHubAuthError,
    GitHubResourceError,
    GitHubRetryableError,
)
from pr_compliance.github.rate_limiter import RateLimiter
from pr_compliance.metrics import RunMetrics

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (GitHubRetryableError, requests.ConnectionError, requests.Timeout)
SKIPPABLE_STATUSES = {404, 409, 410, 422}
API_VERSION = "2022-11-28"


class _WaitRetryAfter:
    """Tenacity wait strategy: honour ``Retry-After`` if the server sent one, otherwise
    exponential backoff with jitter."""

    def __init__(self, fallback: Any, cap_s: float) -> None:
        self.fallback = fallback
        self.cap_s = cap_s

    def __call__(self, state: RetryCallState) -> float:
        exc = state.outcome.exception() if state.outcome else None
        if isinstance(exc, GitHubRetryableError) and exc.retry_after_s is not None:
            return float(min(exc.retry_after_s, self.cap_s))
        return float(self.fallback(state))


class GitHubClient:
    """GitHub REST v3 client.

    * Every request has connect/read timeouts.
    * The rate limiter is consulted before and updated after every response.
    * Transient failures are retried by tenacity; the decision lives in :meth:`_classify`.
    * Pagination follows the ``Link: rel="next"`` header only.
    """

    def __init__(
        self,
        settings: Settings,
        metrics: RunMetrics | None = None,
        session: requests.Session | None = None,
        rate_limiter: RateLimiter | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        if settings.github_token is None or not settings.github_token.get_secret_value():
            raise ConfigurationError(
                "GITHUB_TOKEN is not set. Unauthenticated access is limited to 60 requests/hour"
                " which is not enough for this pipeline; set it in .env."
            )
        self.settings = settings
        self.base_url = settings.github_api_url.rstrip("/")
        self.metrics = metrics or RunMetrics()
        self.rate_limiter = rate_limiter or RateLimiter(
            floor=settings.rate_limit_floor,
            max_sleep_s=settings.rate_limit_max_sleep_s,
            metrics=self.metrics,
        )
        self.timeout = (settings.http_connect_timeout_s, settings.http_read_timeout_s)
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.github_token.get_secret_value()}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "pr-compliance-pipeline",
            }
        )
        self._retrying = Retrying(
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(settings.retry_max_attempts),
            wait=_WaitRetryAfter(
                wait_exponential_jitter(initial=1, max=settings.retry_max_wait_s),
                cap_s=settings.retry_max_wait_s,
            ),
            sleep=sleep,
            before_sleep=self._log_retry,
            reraise=True,
        )

    # ---- public API ----------------------------------------------------------------------
    def get(self, path_or_url: str, params: dict[str, Any] | None = None) -> requests.Response:
        """GET with rate limiting and retries. Raises a :class:`GitHubAPIError` subtype."""
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        return self._retrying(self._get_once, url, params)

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self.get(path, params).json()

    def paginate(
        self, path: str, params: dict[str, Any] | None = None, items_key: str | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield items across all pages by following ``Link: rel="next"``.

        ``items_key`` handles endpoints that wrap the list, e.g. ``check_runs``.
        """
        url: str | None = f"{self.base_url}{path}"
        query: dict[str, Any] | None = {"per_page": 100, **(params or {})}
        while url:
            resp = self.get(url, query)
            payload = resp.json()
            items = payload.get(items_key, []) if items_key else payload
            if not isinstance(items, list):
                raise GitHubAPIError(f"expected a list from {url}", resp.status_code, url)
            yield from items
            url = resp.links.get("next", {}).get("url")
            query = None  # the next URL already carries the query string

    # ---- internals -----------------------------------------------------------------------
    def _get_once(self, url: str, params: dict[str, Any] | None) -> requests.Response:
        self.rate_limiter.wait_if_needed()
        self.metrics.incr("api_calls")
        resp = self.session.get(url, params=params, timeout=self.timeout, allow_redirects=True)
        self.rate_limiter.update(resp.headers)
        self._classify(resp)
        return resp

    def _classify(self, resp: requests.Response) -> None:
        """The single place that decides retry vs fail vs skip for an HTTP response."""
        status = resp.status_code
        if status < 400:
            return
        url = resp.url
        message = _error_message(resp)
        if status == 401:
            raise GitHubAuthError(f"401 Unauthorized: {message}", status, url)
        if status in (403, 429):
            retry_after = resp.headers.get("Retry-After")
            remaining = resp.headers.get("X-RateLimit-Remaining")
            if retry_after is not None:
                # Secondary rate limit: GitHub tells us how long to back off.
                raise GitHubRetryableError(
                    f"{status} secondary rate limit", status, url, retry_after_s=float(retry_after)
                )
            if remaining == "0":
                reset = resp.headers.get("X-RateLimit-Reset")
                self.rate_limiter.mark_exhausted(float(reset) if reset else None)
                # Retry immediately: _get_once will block in wait_if_needed() until reset.
                raise GitHubRetryableError(f"{status} primary rate limit", status, url, 0.0)
            if status == 429:
                raise GitHubRetryableError("429 too many requests", status, url)
            raise GitHubAPIError(f"403 Forbidden: {message}", status, url)
        if status in SKIPPABLE_STATUSES:
            raise GitHubResourceError(f"{status}: {message}", status, url)
        if status >= 500:
            raise GitHubRetryableError(f"{status} server error", status, url)
        raise GitHubAPIError(f"{status}: {message}", status, url)

    def _log_retry(self, state: RetryCallState) -> None:
        self.metrics.incr("api_retries")
        exc = state.outcome.exception() if state.outcome else None
        logger.warning(
            "http_retry",
            extra={
                "attempt": state.attempt_number,
                "error_class": type(exc).__name__ if exc else None,
                "status": getattr(exc, "status", None),
                "sleep_s": round(state.upcoming_sleep, 2),
            },
        )


def _error_message(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return resp.reason or ""
    if isinstance(body, dict):
        return str(body.get("message", resp.reason))
    return resp.reason or ""
