"""Single error hierarchy for the pipeline.

Whether an HTTP failure is retryable is decided in exactly one place
(:meth:`pr_compliance.github.client.GitHubClient._classify`), which raises one of
these types. Everything else just lets them propagate.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for every error raised deliberately by the pipeline."""


class ConfigurationError(PipelineError):
    """Missing or invalid configuration (e.g. no GitHub token)."""


class GitHubAPIError(PipelineError):
    """A GitHub API call failed and will not be retried."""

    def __init__(self, message: str, status: int | None = None, url: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.url = url


class GitHubRetryableError(GitHubAPIError):
    """Transient failure (5xx, 429, secondary rate limit). Retried with backoff."""

    def __init__(
        self,
        message: str,
        status: int | None = None,
        url: str | None = None,
        retry_after_s: float | None = None,
    ) -> None:
        super().__init__(message, status, url)
        self.retry_after_s = retry_after_s


class GitHubAuthError(GitHubAPIError):
    """401: bad or missing token. Never retried - the task fails immediately."""


class GitHubResourceError(GitHubAPIError):
    """404 / 409 / 410 / 422: the resource is gone or not applicable. Skip it."""


class GitHubRateLimitError(GitHubAPIError):
    """The primary rate limit resets later than we are willing to sleep."""


class SchemaValidationError(PipelineError):
    """A JSON schema file is invalid or cannot be loaded."""


class LoadError(PipelineError):
    """Loading into Postgres failed; the transaction was rolled back."""


class DataQualityError(PipelineError):
    """One or more data-quality checks failed."""

    def __init__(self, failures: list[str]) -> None:
        super().__init__("; ".join(failures))
        self.failures = failures
