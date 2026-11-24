"""Bronze -> silver -> gold on real fixture payloads written as a bronze partition."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pr_compliance.storage import Storage
from pr_compliance.transform.gold import ALL_REPOS, GOLD_COLUMNS, GoldBuilder
from pr_compliance.transform.rules import RuleConfig
from pr_compliance.transform.silver import SilverBuilder

DS = date(2026, 10, 5)


def write_bronze(
    storage: Storage,
    fixture: Callable[[str], Any],
    prs: list[dict[str, Any]],
    run_id: str = "r1",
    repo: str = "cli",
) -> Path:
    d = storage.bronze_repo(DS, repo)
    d.mkdir(parents=True)
    by = {"reviews": {}, "commits": {}, "statuses": {}, "check_runs": {}}
    for pr in prs:
        n = pr["number"]
        src = n if n in (699, 681) else 699
        by["reviews"][str(n)] = fixture(f"reviews_{src}.json")
        by["commits"][str(n)] = fixture(f"commits_{src}.json")
        by["statuses"][str(n)] = fixture(f"status_{src}.json")
        by["check_runs"][str(n)] = fixture(f"check_runs_{src}.json")["check_runs"]
    storage.write_json(d / "pull_requests.json", prs)
    for k, v in by.items():
        storage.write_json(d / f"{k}.json", v)
    storage.write_json(
        d / "_manifest.json",
        {
            "repo": repo,
            "organization": "home-assistant",
            "ds": DS.isoformat(),
            "run_id": run_id,
            "status": "success",
            "commits_truncated": [],
            "skipped_prs": [],
        },
    )
    return d


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage(tmp_path / "data")


def pick(fixture: Callable[[str], Any], *numbers: int) -> list[dict[str, Any]]:
    return [copy.deepcopy(p) for p in fixture("pulls_page.json") if p["number"] in numbers]


def test_silver_types_and_flattening(storage: Storage, fixture: Callable[[str], Any]) -> None:
    write_bronze(storage, fixture, pick(fixture, 699, 681))
    result = SilverBuilder(storage).build(DS)
    prs = result.frames["pull_requests"]
    assert len(prs) == 2
    assert str(prs["merged_at"].dtype) == "datetime64[ns, UTC]"
    assert str(prs["pr_number"].dtype) == "Int64"
    assert set(prs["author"]) == {"scop", "dependabot[bot]"}
    assert result.stats["records_quarantined"] == 0
    assert (storage.silver_ds(DS) / "check_runs.parquet").exists()
    statuses = result.frames["statuses"]
    assert set(statuses["context"]) == {"cla-bot", "CodeRabbit"}


def test_invalid_record_is_quarantined(storage: Storage, fixture: Callable[[str], Any]) -> None:
    prs = pick(fixture, 699, 681)
    del prs[1]["title"]
    write_bronze(storage, fixture, prs)
    result = SilverBuilder(storage).build(DS)
    assert len(result.frames["pull_requests"]) == 1
    assert result.stats["records_quarantined"] == 1
    lines = (storage.quarantine_ds(DS) / "pull_requests.jsonl").read_text().splitlines()
    assert "title" in json.loads(lines[0])["errors"][0]


def test_null_user_becomes_ghost(storage: Storage, fixture: Callable[[str], Any]) -> None:
    prs = pick(fixture, 699)
    prs[0]["user"] = None
    write_bronze(storage, fixture, prs)
    result = SilverBuilder(storage).build(DS)
    assert result.frames["pull_requests"].iloc[0]["author"] == "ghost"


def test_gold_end_to_end(storage: Storage, fixture: Callable[[str], Any]) -> None:
    write_bronze(storage, fixture, pick(fixture, 699, 681))
    silver = SilverBuilder(storage).build(DS)
    gold = GoldBuilder(storage, RuleConfig()).build(silver.frames, DS)

    df = pd.read_parquet(gold.pr_compliance_path)
    assert list(df.columns) == GOLD_COLUMNS
    by_pr = df.set_index("pr_number")
    # 699: approved by agners (not the author), 5 green check runs + 2 green statuses
    assert by_pr.loc[699, "code_review_passed"]
    assert by_pr.loc[699, "status_checks_passed"]
    assert by_pr.loc[699, "is_compliant"]
    assert by_pr.loc[699, "commit_count"] == 3
    assert not by_pr.loc[699, "is_bot_author"]
    # 681: dependabot PR approved by frenck
    assert by_pr.loc[681, "is_bot_author"]
    assert by_pr.loc[681, "approved_review_count"] == 1

    summary = json.loads(gold.summary_json_path.read_text())
    assert summary[ALL_REPOS]["total_prs"] == 2
    assert summary[ALL_REPOS]["bot_prs"] == 1
    assert "cli" in summary["repositories"]
    schema = pq.read_schema(gold.pr_compliance_path)
    assert str(schema.field("merged_at").type) == "timestamp[us, tz=UTC]"


def test_gold_rerun_is_idempotent(storage: Storage, fixture: Callable[[str], Any]) -> None:
    write_bronze(storage, fixture, pick(fixture, 699))
    for _ in range(2):
        silver = SilverBuilder(storage).build(DS)
        gold = GoldBuilder(storage, RuleConfig()).build(silver.frames, DS)
    assert len(pd.read_parquet(gold.pr_compliance_path)) == 1
    assert sorted(p.name for p in gold.pr_compliance_path.parent.iterdir()) == [
        "pr_compliance.parquet",
        "summary.json",
        "summary.parquet",
    ]


def test_gold_with_no_prs(storage: Storage, fixture: Callable[[str], Any]) -> None:
    write_bronze(storage, fixture, [])
    silver = SilverBuilder(storage).build(DS)
    gold = GoldBuilder(storage, RuleConfig()).build(silver.frames, DS)
    assert gold.rows == 0
    assert gold.summary[ALL_REPOS]["compliance_rate"] is None


def test_exclude_bots(storage: Storage, fixture: Callable[[str], Any]) -> None:
    write_bronze(storage, fixture, pick(fixture, 699, 681))
    silver = SilverBuilder(storage).build(DS)
    gold = GoldBuilder(storage, RuleConfig(), exclude_bots=True).build(silver.frames, DS)
    assert gold.rows == 1


def test_silver_ignores_partitions_from_other_runs(
    storage: Storage, fixture: Callable[[str], Any]
) -> None:
    write_bronze(storage, fixture, pick(fixture, 699), run_id="current", repo="cli")
    write_bronze(storage, fixture, pick(fixture, 681), run_id="older", repo="version")
    assert set(SilverBuilder(storage).build(DS).frames["pull_requests"]["repository"]) == {
        "cli",
        "version",
    }
    result = SilverBuilder(storage).build(DS, run_id="current")
    assert set(result.frames["pull_requests"]["repository"]) == {"cli"}
