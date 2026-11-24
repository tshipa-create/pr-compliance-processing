"""Data-quality checks that run after load and fail the DAG run (which alerts)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd

from pr_compliance.config import Settings
from pr_compliance.exceptions import DataQualityError
from pr_compliance.metrics import ConnectionFactory
from pr_compliance.storage import Storage
from pr_compliance.transform.gold import LoadScope
from pr_compliance.transform.silver import USABLE_MANIFEST_STATUSES

logger = logging.getLogger(__name__)

HISTORY_DAYS = 7
MIN_HISTORY_FOR_TRENDS = 3


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityChecker:
    def __init__(
        self, settings: Settings, storage: Storage, connection_factory: ConnectionFactory
    ) -> None:
        self.settings = settings
        self.storage = storage
        self._factory = connection_factory

    def run(
        self, ds: date, run_id: str, expected_repos: list[str], window_end: datetime
    ) -> list[CheckResult]:
        """Run every check; raise :class:`DataQualityError` listing all failures."""
        gold_path = self.storage.gold_ds(ds) / "pr_compliance.parquet"
        gold = pd.read_parquet(gold_path) if gold_path.exists() else pd.DataFrame()
        history = self._history(ds)
        results = [
            self.check_completeness(ds, run_id, expected_repos),
            self.check_quarantine_rate(ds),
            self.check_volume(len(gold), history),
            self.check_drift(gold, history),
            self.check_freshness(window_end),
            self.check_integrity(ds, len(gold)),
        ]
        for r in results:
            log = logger.info if r.passed else logger.error
            log("dq_check", extra={"check": r.name, "passed": r.passed, "detail": r.detail})
        failures = [f"{r.name}: {r.detail}" for r in results if not r.passed]
        if failures:
            raise DataQualityError(failures)
        return results

    # ---- individual checks ---------------------------------------------------------------
    def check_completeness(self, ds: date, run_id: str, expected: list[str]) -> CheckResult:
        """Every listed repo has a usable manifest written by *this* run."""
        missing = []
        for repo in expected:
            path = self.storage.bronze_repo(ds, repo) / "_manifest.json"
            if not path.exists():
                missing.append(repo)
                continue
            manifest = self.storage.read_json(path)
            if manifest["run_id"] != run_id or manifest["status"] not in USABLE_MANIFEST_STATUSES:
                missing.append(repo)
        if missing:
            return CheckResult("completeness", False, f"extract failed for {sorted(missing)}")
        return CheckResult("completeness", True, f"{len(expected)} repos extracted")

    def check_quarantine_rate(self, ds: date) -> CheckResult:
        stats_path = self.storage.silver_ds(ds) / "_stats.json"
        if not stats_path.exists():
            return CheckResult("quarantine_rate", False, "silver _stats.json missing")
        rate = float(self.storage.read_json(stats_path)["quarantine_rate"])
        limit = self.settings.dq_max_quarantine_rate
        return CheckResult("quarantine_rate", rate <= limit, f"{rate:.2%} (max {limit:.0%})")

    def check_volume(self, rows: int, history: pd.DataFrame) -> CheckResult:
        if rows < self.settings.dq_min_prs:
            return CheckResult("volume", False, f"{rows} PRs < minimum {self.settings.dq_min_prs}")
        if len(history) >= MIN_HISTORY_FOR_TRENDS:
            avg = float(history["total_prs"].mean())
            floor = avg * self.settings.dq_volume_drop_ratio
            if avg > 0 and rows < floor:
                return CheckResult("volume", False, f"{rows} PRs < {floor:.0f} ({avg:.0f} avg)")
        return CheckResult("volume", True, f"{rows} PRs")

    def check_drift(self, gold: pd.DataFrame, history: pd.DataFrame) -> CheckResult:
        if gold.empty or len(history) < MIN_HISTORY_FOR_TRENDS:
            return CheckResult("compliance_drift", True, "not enough history")
        rate = float(gold["is_compliant"].mean())
        avg = float(history["compliance_rate"].dropna().astype(float).mean())
        drift_pts = abs(rate - avg) * 100
        limit = self.settings.dq_max_compliance_drift_pts
        return CheckResult(
            "compliance_drift",
            drift_pts <= limit,
            f"{rate:.1%} vs {avg:.1%} 7-day avg ({drift_pts:.1f} pts, max {limit:.0f})",
        )

    def check_freshness(self, window_end: datetime) -> CheckResult:
        newest = self._scalar("SELECT max(merged_at) FROM pr_compliance")
        if newest is None:
            return CheckResult("freshness", False, "pr_compliance is empty")
        age_h = (window_end - newest).total_seconds() / 3600
        limit = self.settings.dq_freshness_hours
        return CheckResult(
            "freshness", age_h <= limit, f"newest merge {age_h:.1f}h old (max {limit:.0f}h)"
        )

    def check_integrity(self, ds: date, gold_rows: int) -> CheckResult:
        """Postgres holds exactly the gold rows for this run's scope (repos x merge
        window): catches partial loads and orphans left behind by earlier runs."""
        scope_path = self.storage.gold_ds(ds) / "load_scope.json"
        if not scope_path.exists():
            return CheckResult("integrity", False, "gold load_scope.json missing")
        scope = LoadScope.read(scope_path)
        loaded = self._scalar(
            "SELECT count(*) FROM pr_compliance "
            "WHERE repository = ANY(%s) AND merged_at >= %s AND merged_at < %s",
            (scope.repos, scope.window_start, scope.window_end),
        )
        ok = int(loaded or 0) == gold_rows
        return CheckResult("integrity", ok, f"{loaded} rows in Postgres vs {gold_rows} in gold")

    # ---- DB helpers ----------------------------------------------------------------------
    def _history(self, ds: date) -> pd.DataFrame:
        sql = (
            "SELECT run_date, total_prs, compliance_rate FROM compliance_summary "
            "WHERE repository = '__all__' AND run_date < %s AND run_date >= %s::date - %s "
            "ORDER BY run_date"
        )
        conn = self._factory()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (ds, ds, HISTORY_DAYS))
                rows = cur.fetchall()
        finally:
            conn.close()
        return pd.DataFrame(rows, columns=["run_date", "total_prs", "compliance_rate"])

    def _scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        conn = self._factory()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        finally:
            conn.close()
        return row[0] if row else None
