from __future__ import annotations

import pytest
import requests
import responses

from pr_compliance.config import Settings
from pr_compliance.exceptions import (
    ConfigurationError,
    GitHubAPIError,
    GitHubAuthError,
    GitHubResourceError,
    GitHubRetryableError,
)
from pr_compliance.github.client import GitHubClient
from tests.conftest import API

URL = f"{API}/repos/o/r/pulls"


def link(page: int) -> dict[str, str]:
    return {"Link": f'<{URL}?per_page=100&page={page}>; rel="next"'}


@responses.activate
def test_paginate_follows_link_header(client: GitHubClient) -> None:
    responses.get(URL, json=[{"n": 1}, {"n": 2}], headers=link(2))
    responses.get(f"{URL}?per_page=100&page=2", json=[{"n": 3}], headers=link(3))
    responses.get(f"{URL}?per_page=100&page=3", json=[{"n": 4}])
    assert [i["n"] for i in client.paginate("/repos/o/r/pulls")] == [1, 2, 3, 4]
    assert len(responses.calls) == 3
    # first request carries per_page, later ones reuse the server-provided URL verbatim
    assert "per_page=100" in responses.calls[0].request.url
    assert client.metrics.api_calls == 3


@responses.activate
def test_paginate_empty_first_page(client: GitHubClient) -> None:
    responses.get(URL, json=[])
    assert list(client.paginate("/repos/o/r/pulls")) == []


@responses.activate
def test_paginate_wrapped_items(client: GitHubClient) -> None:
    url = f"{API}/repos/o/r/commits/abc/check-runs"
    responses.get(url, json={"total_count": 1, "check_runs": [{"id": 1}]})
    assert list(client.paginate("/repos/o/r/commits/abc/check-runs", items_key="check_runs")) == [
        {"id": 1}
    ]


@responses.activate
def test_retries_5xx_then_succeeds(client: GitHubClient, sleeps: list[float]) -> None:
    responses.get(URL, status=502)
    responses.get(URL, status=502)
    responses.get(URL, json=[{"ok": True}])
    assert client.get_json("/repos/o/r/pulls") == [{"ok": True}]
    assert client.metrics.api_calls == 3
    assert client.metrics.api_retries == 2
    assert len(sleeps) == 2  # exponential backoff waits


@responses.activate
def test_gives_up_after_max_attempts(client: GitHubClient) -> None:
    for _ in range(3):
        responses.get(URL, status=503)
    with pytest.raises(GitHubRetryableError):
        client.get("/repos/o/r/pulls")
    assert client.metrics.api_calls == 3


@responses.activate
def test_retries_connection_error(client: GitHubClient) -> None:
    responses.get(URL, body=requests.ConnectionError("boom"))
    responses.get(URL, json=[])
    assert client.get_json("/repos/o/r/pulls") == []


@responses.activate
def test_401_fails_fast(client: GitHubClient) -> None:
    responses.get(URL, status=401, json={"message": "Bad credentials"})
    with pytest.raises(GitHubAuthError, match="Bad credentials"):
        client.get("/repos/o/r/pulls")
    assert client.metrics.api_calls == 1


@pytest.mark.parametrize("status", [404, 409, 422])
@responses.activate
def test_resource_errors_not_retried(client: GitHubClient, status: int) -> None:
    responses.get(URL, status=status, json={"message": "nope"})
    with pytest.raises(GitHubResourceError) as info:
        client.get("/repos/o/r/pulls")
    assert info.value.status == status
    assert client.metrics.api_calls == 1


@responses.activate
def test_secondary_rate_limit_honours_retry_after(
    client: GitHubClient, sleeps: list[float]
) -> None:
    responses.get(URL, status=403, headers={"Retry-After": "7"}, json={"message": "secondary"})
    responses.get(URL, json=[])
    client.get("/repos/o/r/pulls")
    assert sleeps == [7.0]


@responses.activate
def test_429_retried(client: GitHubClient) -> None:
    responses.get(URL, status=429)
    responses.get(URL, json=[])
    client.get("/repos/o/r/pulls")
    assert client.metrics.api_retries == 1


@responses.activate
def test_primary_rate_limit_exhausted_waits_for_reset(
    client: GitHubClient, sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000.0
    monkeypatch.setattr(client.rate_limiter, "_clock", lambda: now)
    responses.get(
        URL,
        status=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(now + 60))},
        json={"message": "API rate limit exceeded"},
    )
    responses.get(
        URL, json=[], headers={"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": "9e9"}
    )
    client.get("/repos/o/r/pulls")
    # tenacity waits 0s (retry_after=0), then the limiter sleeps until reset + buffer
    assert pytest.approx(62.0) in sleeps


@responses.activate
def test_plain_403_is_not_retried(client: GitHubClient) -> None:
    responses.get(URL, status=403, json={"message": "Resource not accessible"})
    with pytest.raises(GitHubAPIError) as info:
        client.get("/repos/o/r/pulls")
    assert not isinstance(info.value, GitHubRetryableError)


@responses.activate
def test_rate_limit_headers_tracked(client: GitHubClient) -> None:
    responses.get(URL, json=[], headers={"X-RateLimit-Remaining": "4321", "X-RateLimit-Reset": "1"})
    client.get("/repos/o/r/pulls")
    assert client.metrics.min_rate_limit_remaining == 4321


@responses.activate
def test_token_sent_and_never_in_errors(client: GitHubClient) -> None:
    responses.get(URL, status=401, json={"message": "Bad credentials"})
    with pytest.raises(GitHubAuthError) as info:
        client.get("/repos/o/r/pulls")
    assert responses.calls[0].request.headers["Authorization"] == "Bearer test-token"
    assert "test-token" not in str(info.value)


def test_missing_token_is_a_configuration_error(tmp_path: object) -> None:
    with pytest.raises(ConfigurationError, match="GITHUB_TOKEN"):
        GitHubClient(Settings(github_token=None))
