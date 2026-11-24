from __future__ import annotations

import copy
import json
from collections.abc import Callable
from typing import Any

import pytest
import responses

from pr_compliance.config import Settings
from pr_compliance.exceptions import GitHubAuthError
from pr_compliance.extract.extractor import PRExtractor
from pr_compliance.github.client import GitHubClient
from pr_compliance.github.models import Window
from pr_compliance.storage import Storage
from tests.conftest import API

REPO = "cli"
BASE = f"{API}/repos/home-assistant/{REPO}"


def mock_pr_details(
    fixture: Callable[[str], Any], pulls: list[dict[str, Any]], numbers: list[int]
) -> None:
    """Register detail endpoints; PRs other than 699/681 get 699's payloads."""
    for pr in pulls:
        n = pr["number"]
        if n not in numbers:
            continue
        src = n if n in (699, 681) else 699
        sha = pr["head"]["sha"]
        responses.get(f"{BASE}/pulls/{n}/reviews", json=fixture(f"reviews_{src}.json"))
        responses.get(f"{BASE}/pulls/{n}/commits", json=fixture(f"commits_{src}.json"))
        responses.get(f"{BASE}/commits/{sha}/status", json=fixture(f"status_{src}.json"))
        responses.get(f"{BASE}/commits/{sha}/check-runs", json=fixture(f"check_runs_{src}.json"))


@pytest.fixture
def extractor(client: GitHubClient, settings: Settings) -> PRExtractor:
    return PRExtractor(client, Storage(settings.data_dir), settings)


@responses.activate
def test_window_filter_and_early_stop(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window
) -> None:
    pulls = fixture("pulls_page.json")
    next_url = f"{BASE}/pulls?page=2"
    responses.get(f"{BASE}/pulls", json=pulls, headers={"Link": f'<{next_url}>; rel="next"'})
    page2 = responses.get(next_url, json=[])
    merged = [p["number"] for p in extractor.iter_merged_prs(REPO, window)]
    # merged inside [2026-11-12, 2026-11-17); unmerged 698/697/691 excluded; the page
    # contains PRs updated before the window start so pagination stops before page 2.
    assert merged == [699, 695, 696, 694, 693, 692]
    assert page2.call_count == 0


@responses.activate
def test_extract_repo_writes_bronze_and_manifest(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window, settings: Settings
) -> None:
    pulls = fixture("pulls_page.json")
    responses.get(f"{BASE}/pulls", json=pulls)
    mock_pr_details(fixture, pulls, [699, 695, 696, 694, 693, 692])

    path = extractor.extract_repo(REPO, window, run_id="manual__1")

    assert path == settings.data_dir / "bronze" / "ds=2026-11-12" / "repo=cli"
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["status"] == "success"
    assert manifest["pr_count"] == 6
    assert manifest["run_id"] == "manual__1"
    assert manifest["skipped_prs"] == []
    reviews = json.loads((path / "reviews.json").read_text())
    assert set(reviews) == {"699", "695", "696", "694", "693", "692"}
    assert extractor.client.metrics.prs_fetched == 6
    # no leftover staging directories
    assert [p.name for p in path.parent.iterdir()] == ["repo=cli"]


@responses.activate
def test_resource_404_skips_only_that_resource(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window
) -> None:
    pulls = [p for p in fixture("pulls_page.json") if p["number"] == 699]
    responses.get(f"{BASE}/pulls", json=pulls)
    sha = pulls[0]["head"]["sha"]
    responses.get(f"{BASE}/pulls/699/reviews", json=fixture("reviews_699.json"))
    responses.get(f"{BASE}/pulls/699/commits", json=fixture("commits_699.json"))
    responses.get(f"{BASE}/commits/{sha}/status", status=404, json={"message": "Not Found"})
    responses.get(f"{BASE}/commits/{sha}/check-runs", json=fixture("check_runs_699.json"))

    path = extractor.extract_repo(REPO, window, run_id="r")
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["pr_count"] == 1
    assert manifest["skipped_prs"][0]["resource"] == "statuses"
    assert manifest["skipped_prs"][0]["status"] == 404
    assert json.loads((path / "statuses.json").read_text()) == {}


@responses.activate
def test_empty_repo_409_is_skipped(extractor: PRExtractor, window: Window) -> None:
    responses.get(f"{BASE}/pulls", status=409, json={"message": "Git Repository is empty."})
    path = extractor.extract_repo(REPO, window, run_id="r")
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["status"] == "skipped_empty_repo"
    assert manifest["pr_count"] == 0


@responses.activate
def test_auth_error_fails_repo_and_keeps_previous_partition(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window, settings: Settings
) -> None:
    previous = settings.data_dir / "bronze" / "ds=2026-11-12" / "repo=cli"
    previous.mkdir(parents=True)
    (previous / "_manifest.json").write_text('{"old": true}')
    pulls = [p for p in fixture("pulls_page.json") if p["number"] == 699]
    responses.get(f"{BASE}/pulls", json=pulls)
    responses.get(f"{BASE}/pulls/699/reviews", status=401, json={"message": "Bad credentials"})
    responses.get(f"{BASE}/pulls/699/commits", json=[])
    sha = pulls[0]["head"]["sha"]
    responses.get(f"{BASE}/commits/{sha}/status", json=fixture("status_699.json"))
    responses.get(f"{BASE}/commits/{sha}/check-runs", json=fixture("check_runs_699.json"))

    with pytest.raises(GitHubAuthError):
        extractor.extract_repo(REPO, window, run_id="r")
    assert json.loads((previous / "_manifest.json").read_text()) == {"old": True}
    assert [p.name for p in previous.parent.iterdir()] == ["repo=cli"]


@responses.activate
def test_edge_payloads_deleted_user_and_fork(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window
) -> None:
    pr = copy.deepcopy(next(p for p in fixture("pulls_page.json") if p["number"] == 699))
    pr["user"] = None
    pr["head"]["repo"] = None
    responses.get(f"{BASE}/pulls", json=[pr])
    mock_pr_details(fixture, [pr], [699])
    path = extractor.extract_repo(REPO, window, run_id="r")
    stored = json.loads((path / "pull_requests.json").read_text())
    assert stored[0]["user"] is None


@responses.activate
def test_commit_cap_flagged(
    extractor: PRExtractor, fixture: Callable[[str], Any], window: Window
) -> None:
    pr = next(p for p in fixture("pulls_page.json") if p["number"] == 699)
    responses.get(f"{BASE}/pulls", json=[pr])
    commit = fixture("commits_699.json")[0]
    commits_url = f"{BASE}/pulls/699/commits"
    responses.get(
        commits_url, json=[commit] * 100, headers={"Link": f'<{commits_url}?p=2>; rel="next"'}
    )
    responses.get(
        f"{commits_url}?p=2",
        json=[commit] * 100,
        headers={"Link": f'<{commits_url}?p=3>; rel="next"'},
    )
    responses.get(f"{commits_url}?p=3", json=[commit] * 50)
    responses.get(f"{BASE}/pulls/699/reviews", json=[])
    sha = pr["head"]["sha"]
    responses.get(f"{BASE}/commits/{sha}/status", json=fixture("status_699.json"))
    responses.get(f"{BASE}/commits/{sha}/check-runs", json=fixture("check_runs_699.json"))
    path = extractor.extract_repo(REPO, window, run_id="r")
    assert json.loads((path / "_manifest.json").read_text())["commits_truncated"] == [699]


@responses.activate
def test_list_repos_skips_archived_and_empty(extractor: PRExtractor) -> None:
    responses.get(
        f"{API}/orgs/home-assistant/repos",
        json=[
            {"name": "core", "archived": False, "size": 10},
            {"name": "old", "archived": True, "size": 10},
            {"name": "empty", "archived": False, "size": 0},
        ],
    )
    assert extractor.list_repos() == ["core"]


def test_list_repos_allowlist(settings: Settings, client: GitHubClient) -> None:
    s = settings.model_copy(update={"repo_allowlist": ["b", "a"]})
    assert PRExtractor(client, Storage(s.data_dir), s).list_repos() == ["a", "b"]


def test_allowlist_parsed_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPO_ALLOWLIST", "cli, version ,")
    assert Settings().repo_allowlist == ["cli", "version"]


def test_window_for_manual_trigger_is_previous_utc_day() -> None:
    from datetime import UTC, date, datetime

    w = Window.previous_day(datetime(2026, 11, 24, 19, 22, tzinfo=UTC))
    assert (w.start, w.end, w.ds) == (
        datetime(2026, 11, 23, tzinfo=UTC),
        datetime(2026, 11, 24, tzinfo=UTC),
        date(2026, 11, 23),
    )
    wide = Window.previous_day(datetime(2026, 11, 24, 1, tzinfo=UTC), lookback_days=7)
    assert wide.start == datetime(2026, 11, 17, tzinfo=UTC) and wide.ds == date(2026, 11, 23)
