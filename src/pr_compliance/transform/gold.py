"""Silver -> gold: the ``pr_compliance`` table and summary statistics."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from pr_compliance.github.models import CHECK_RUNS, COMMITS, PULL_REQUESTS, REVIEWS, STATUSES
from pr_compliance.storage import Storage
from pr_compliance.transform.rules import KEY, RuleConfig, evaluate

try:  # pandera >= 0.24 moved the pandas API to pandera.pandas
    import pandera.pandas as pda
except ImportError:  # pragma: no cover
    import pandera as pda  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

ALL_REPOS = "__all__"

GOLD_COLUMNS = [
    "organization",
    "repository",
    "pr_number",
    "pr_title",
    "author",
    "author_id",
    "is_bot_author",
    "base_ref",
    "head_ref",
    "merged_at",
    "approved_review_count",
    "commit_count",
    "commits_truncated",
    "code_review_passed",
    "status_checks_passed",
    "is_compliant",
    "violation_reasons",
    "run_date",
]

GOLD_ARROW_SCHEMA = pa.schema(
    [
        ("organization", pa.string()),
        ("repository", pa.string()),
        ("pr_number", pa.int64()),
        ("pr_title", pa.string()),
        ("author", pa.string()),
        ("author_id", pa.int64()),
        ("is_bot_author", pa.bool_()),
        ("base_ref", pa.string()),
        ("head_ref", pa.string()),
        ("merged_at", pa.timestamp("us", tz="UTC")),
        ("approved_review_count", pa.int64()),
        ("commit_count", pa.int64()),
        ("commits_truncated", pa.bool_()),
        ("code_review_passed", pa.bool_()),
        ("status_checks_passed", pa.bool_()),
        ("is_compliant", pa.bool_()),
        ("violation_reasons", pa.list_(pa.string())),
        ("run_date", pa.date32()),
    ]
)

GOLD_SCHEMA = pda.DataFrameSchema(
    {
        "organization": pda.Column(str, nullable=False),
        "repository": pda.Column(str, nullable=False),
        "pr_number": pda.Column("int64", pda.Check.gt(0), nullable=False),
        "pr_title": pda.Column(str, nullable=False),
        "author": pda.Column(str, nullable=False),
        "author_id": pda.Column("Int64", nullable=True),
        "is_bot_author": pda.Column(bool),
        "merged_at": pda.Column("datetime64[ns, UTC]", nullable=False),
        "approved_review_count": pda.Column("int64", pda.Check.ge(0)),
        "commit_count": pda.Column("int64", pda.Check.ge(0)),
        "code_review_passed": pda.Column(bool),
        "status_checks_passed": pda.Column(bool),
        "is_compliant": pda.Column(bool),
        "violation_reasons": pda.Column(
            object, pda.Check(lambda s: s.map(lambda v: isinstance(v, list)), element_wise=False)
        ),
    },
    unique=KEY,
    strict=False,
    coerce=False,
)


@dataclass(frozen=True)
class LoadScope:
    """What this run owns in the output table: PRs of ``repos`` merged in the window.

    Load replaces exactly this slice (upsert, then delete rows no longer in gold), so a
    rerun with a narrower window or fewer repos leaves no orphans, and repos that failed
    extraction this run (absent from ``repos``) keep their previously loaded rows.
    """

    window_start: datetime
    window_end: datetime
    repos: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "repos": sorted(self.repos),
        }

    @classmethod
    def read(cls, path: Path) -> LoadScope:
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            window_start=datetime.fromisoformat(data["window_start"]),
            window_end=datetime.fromisoformat(data["window_end"]),
            repos=list(data["repos"]),
        )


@dataclass
class GoldResult:
    pr_compliance_path: Path
    summary_path: Path
    summary_json_path: Path
    load_scope_path: Path
    rows: int
    summary: dict[str, Any]

    def as_xcom(self) -> dict[str, str]:
        return {
            "pr_compliance": str(self.pr_compliance_path),
            "summary": str(self.summary_path),
            "summary_json": str(self.summary_json_path),
            "load_scope": str(self.load_scope_path),
        }


def is_bot(login: pd.Series, user_type: pd.Series) -> pd.Series:
    return (user_type.fillna("") == "Bot") | login.fillna("").str.endswith("[bot]")


def build_gold_frame(
    frames: dict[str, pd.DataFrame], ds: date, cfg: RuleConfig, exclude_bots: bool = False
) -> pd.DataFrame:
    """Combine silver entities and rule outputs into the gold ``pr_compliance`` frame."""
    prs = frames[PULL_REQUESTS]
    rules = evaluate(prs, frames[REVIEWS], frames[CHECK_RUNS], frames[STATUSES], cfg)
    commit_counts = frames[COMMITS].groupby(KEY).size().rename("commit_count")

    gold = prs.merge(rules, on=KEY, how="left")
    gold = gold.merge(commit_counts, left_on=KEY, right_index=True, how="left")
    gold["commit_count"] = gold["commit_count"].fillna(0).astype("int64")
    gold["is_bot_author"] = is_bot(gold["author"], gold["author_type"]).astype(bool)
    gold["run_date"] = ds
    if exclude_bots:
        gold = gold[~gold["is_bot_author"]]

    gold = gold.astype(
        {
            "pr_number": "int64",
            "approved_review_count": "int64",
            "organization": object,
            "repository": object,
            "pr_title": object,
            "author": object,
            "base_ref": object,
            "head_ref": object,
            "commits_truncated": bool,
            "code_review_passed": bool,
            "status_checks_passed": bool,
            "is_compliant": bool,
        }
    )
    gold = gold[GOLD_COLUMNS].sort_values(KEY).reset_index(drop=True)
    validated: pd.DataFrame = GOLD_SCHEMA.validate(gold, lazy=True)
    return validated


def _group_stats(df: pd.DataFrame) -> dict[str, Any]:
    total = len(df)
    compliant = int(df["is_compliant"].sum())
    humans = df[~df["is_bot_author"]]
    human_total, human_compliant = len(humans), int(humans["is_compliant"].sum())
    violations = Counter(reason for reasons in df["violation_reasons"] for reason in reasons)
    return {
        "total_prs": total,
        "compliant_prs": compliant,
        "non_compliant_prs": total - compliant,
        "compliance_rate": round(compliant / total, 4) if total else None,
        "violations": dict(sorted(violations.items())),
        "human_prs": human_total,
        "human_compliant_prs": human_compliant,
        "human_compliance_rate": round(human_compliant / human_total, 4) if human_total else None,
        "bot_prs": total - human_total,
    }


def build_summary(gold: pd.DataFrame, ds: date) -> dict[str, Any]:
    """Org totals plus per-repository stats (violations by reason, humans-only rate)."""
    by_repo = {repo: _group_stats(g) for repo, g in gold.groupby("repository", sort=True)}
    return {"run_date": ds.isoformat(), ALL_REPOS: _group_stats(gold), "repositories": by_repo}


def summary_frame(summary: dict[str, Any]) -> pd.DataFrame:
    rows = [{"repository": ALL_REPOS, **summary[ALL_REPOS]}]
    rows += [{"repository": repo, **stats} for repo, stats in summary["repositories"].items()]
    df = pd.DataFrame(rows)
    df.insert(0, "run_date", pd.to_datetime(summary["run_date"]).date())
    df["violations"] = df["violations"].map(json.dumps)
    return df


class GoldBuilder:
    def __init__(self, storage: Storage, cfg: RuleConfig, exclude_bots: bool = False) -> None:
        self.storage = storage
        self.cfg = cfg
        self.exclude_bots = exclude_bots

    def build(
        self, frames: dict[str, pd.DataFrame], ds: date, scope: LoadScope | None = None
    ) -> GoldResult:
        gold = build_gold_frame(frames, ds, self.cfg, self.exclude_bots)
        summary = build_summary(gold, ds)
        out = self.storage.reset_dir(self.storage.gold_ds(ds))
        prc = self.storage.write_parquet(out / "pr_compliance.parquet", gold, GOLD_ARROW_SCHEMA)
        summ = self.storage.write_parquet(out / "summary.parquet", summary_frame(summary))
        summ_json = self.storage.write_json(out / "summary.json", summary)
        scope_path = out / "load_scope.json"
        if scope is not None:
            self.storage.write_json(scope_path, scope.to_json())
        org = summary[ALL_REPOS]
        logger.info(
            "gold_written",
            extra={
                "rows": len(gold),
                "compliance_rate": org["compliance_rate"],
                "human_compliance_rate": org["human_compliance_rate"],
                "violations": org["violations"],
            },
        )
        return GoldResult(prc, summ, summ_json, scope_path, len(gold), summary)
