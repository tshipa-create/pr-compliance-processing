"""Shared fixtures. Payloads in tests/fixtures are trimmed *real* GitHub responses from
home-assistant/cli (PR #699 by a human, PR #681 by dependabot)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from pr_compliance.config import Settings
from pr_compliance.github.client import GitHubClient
from pr_compliance.github.models import Window
from pr_compliance.github.rate_limiter import RateLimiter
from pr_compliance.metrics import RunMetrics

FIXTURES = Path(__file__).parent / "fixtures"
API = "https://api.github.com"


@pytest.fixture
def fixture() -> Callable[[str], Any]:
    def _load(name: str) -> Any:
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    return _load


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        github_token="test-token",
        github_org="home-assistant",
        data_dir=tmp_path / "data",
        extract_max_workers=2,
        retry_max_attempts=3,
    )


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def client(settings: Settings, sleeps: list[float]) -> GitHubClient:
    metrics = RunMetrics()
    limiter = RateLimiter(floor=settings.rate_limit_floor, metrics=metrics, sleep=sleeps.append)
    return GitHubClient(settings, metrics, rate_limiter=limiter, sleep=sleeps.append)


@pytest.fixture
def window() -> Window:
    # PR #699 merged 2026-11-16T13:10:08Z, #695 2026-11-12T13:58:50Z
    return Window(
        start=datetime(2026, 11, 12, tzinfo=UTC),
        end=datetime(2026, 11, 17, tzinfo=UTC),
        ds=date(2026, 11, 12),
    )
