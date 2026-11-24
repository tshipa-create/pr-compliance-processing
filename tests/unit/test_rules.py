"""Compliance rules: pure functions, so these tests build tiny DataFrames directly."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from pr_compliance.transform.rules import (
    FAILED_CHECKS,
    NO_APPROVAL,
    NO_CHECKS,
    SELF_APPROVED_ONLY,
    RuleConfig,
    evaluate,
    evaluate_checks,
    evaluate_reviews,
)

MERGED = pd.Timestamp("2026-11-16T13:00:00Z")
CFG = RuleConfig()


def prs(author: str = "alice") -> pd.DataFrame:
    return pd.DataFrame(
        [{"repository": "r", "pr_number": 1, "author": author, "merged_at": MERGED}]
    )


def reviews(*rows: tuple[str, str, str]) -> pd.DataFrame:
    """rows: (reviewer, state, submitted_at)"""
    return pd.DataFrame(
        [
            {
                "repository": "r",
                "pr_number": 1,
                "review_id": i,
                "reviewer": who,
                "state": state,
                "submitted_at": pd.Timestamp(ts),
            }
            for i, (who, state, ts) in enumerate(rows)
        ],
        columns=["repository", "pr_number", "review_id", "reviewer", "state", "submitted_at"],
    )


def check_runs(*rows: tuple[int, str, str, str | None]) -> pd.DataFrame:
    """rows: (id, name, status, conclusion)"""
    return pd.DataFrame(
        [
            {
                "repository": "r",
                "pr_number": 1,
                "check_run_id": i,
                "name": n,
                "status": s,
                "conclusion": c,
                "app_slug": "github-actions",
            }
            for i, n, s, c in rows
        ],
        columns=[
            "repository",
            "pr_number",
            "check_run_id",
            "name",
            "status",
            "conclusion",
            "app_slug",
        ],
    )


def statuses(*rows: tuple[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"repository": "r", "pr_number": 1, "context": c, "status_state": s} for c, s in rows],
        columns=["repository", "pr_number", "context", "status_state"],
    )


def review_result(rv: pd.DataFrame, cfg: RuleConfig = CFG, author: str = "alice") -> Any:
    return evaluate_reviews(prs(author), rv, cfg).iloc[0]


# ---- code review -------------------------------------------------------------------------
def test_single_approval_passes() -> None:
    r = review_result(reviews(("bob", "APPROVED", "2026-11-16T12:00Z")))
    assert r.code_review_passed and r.approved_review_count == 1


def test_no_reviews_fails() -> None:
    r = review_result(reviews())
    assert not r.code_review_passed and r.approved_review_count == 0


def test_approve_then_changes_requested_fails() -> None:
    r = review_result(
        reviews(
            ("bob", "APPROVED", "2026-11-16T10:00Z"),
            ("bob", "CHANGES_REQUESTED", "2026-11-16T11:00Z"),
        )
    )
    assert not r.code_review_passed


def test_comment_after_approval_keeps_approval() -> None:
    r = review_result(
        reviews(("bob", "APPROVED", "2026-11-16T10:00Z"), ("bob", "COMMENTED", "2026-11-16T11:00Z"))
    )
    assert r.code_review_passed


def test_only_author_approved_fails() -> None:
    r = review_result(reviews(("alice", "APPROVED", "2026-11-16T10:00Z")))
    assert not r.code_review_passed and r.self_approved


def test_dismissed_approval_fails() -> None:
    r = review_result(reviews(("bob", "DISMISSED", "2026-11-16T10:00Z")))
    assert not r.code_review_passed


def test_two_reviewers_one_approves_passes() -> None:
    r = review_result(
        reviews(
            ("bob", "CHANGES_REQUESTED", "2026-11-16T10:00Z"),
            ("carol", "APPROVED", "2026-11-16T11:00Z"),
        )
    )
    assert r.code_review_passed and r.approved_review_count == 1


def test_approval_after_merge_ignored() -> None:
    r = review_result(reviews(("bob", "APPROVED", "2026-11-17T10:00Z")))
    assert not r.code_review_passed


def test_min_approvals_configurable() -> None:
    rv = reviews(("bob", "APPROVED", "2026-11-16T10:00Z"))
    assert not review_result(rv, RuleConfig(min_approvals=2)).code_review_passed
    rv2 = reviews(("bob", "APPROVED", "2026-11-16T10:00Z"), ("eve", "APPROVED", "2026-11-16T11Z"))
    assert review_result(rv2, RuleConfig(min_approvals=2)).code_review_passed


# ---- status checks -----------------------------------------------------------------------
def checks_result(cr: pd.DataFrame, st: pd.DataFrame | None = None, cfg: RuleConfig = CFG) -> Any:
    return evaluate_checks(prs(), cr, st if st is not None else statuses(), cfg).iloc[0]


def test_all_success_passes() -> None:
    r = checks_result(
        check_runs((1, "lint", "completed", "success"), (2, "test", "completed", "success"))
    )
    assert r.status_checks_passed and r.check_count == 2


def test_one_failure_fails() -> None:
    r = checks_result(
        check_runs((1, "lint", "completed", "success"), (2, "test", "completed", "failure"))
    )
    assert not r.status_checks_passed and r.failed_check_count == 1


def test_failed_then_rerun_success_passes() -> None:
    r = checks_result(
        check_runs((1, "test", "completed", "failure"), (2, "test", "completed", "success"))
    )
    assert r.status_checks_passed and r.check_count == 1


@pytest.mark.parametrize("conclusion", ["skipped", "neutral"])
def test_skipped_and_neutral_pass(conclusion: str) -> None:
    assert checks_result(check_runs((1, "x", "completed", conclusion))).status_checks_passed


@pytest.mark.parametrize("conclusion", ["cancelled", "timed_out", "action_required", "stale"])
def test_bad_conclusions_fail(conclusion: str) -> None:
    assert not checks_result(check_runs((1, "x", "completed", conclusion))).status_checks_passed


def test_in_progress_at_merge_fails() -> None:
    assert not checks_result(check_runs((1, "x", "in_progress", None))).status_checks_passed


def test_no_checks_fails_unless_configured() -> None:
    assert not checks_result(check_runs()).status_checks_passed
    assert checks_result(check_runs(), cfg=RuleConfig(no_checks_passes=True)).status_checks_passed


def test_legacy_status_failure_fails() -> None:
    r = checks_result(
        check_runs((1, "x", "completed", "success")), statuses(("ci/jenkins", "failure"))
    )
    assert not r.status_checks_passed


def test_legacy_statuses_alone_count_as_checks() -> None:
    r = checks_result(check_runs(), statuses(("cla-bot", "success")))
    assert r.status_checks_passed and r.check_count == 1


# ---- combined ----------------------------------------------------------------------------
def test_evaluate_reasons() -> None:
    out = evaluate(
        prs(), reviews(("alice", "APPROVED", "2026-11-16T10:00Z")), check_runs(), statuses(), CFG
    )
    row = out.iloc[0]
    assert not row.is_compliant
    assert row.violation_reasons == [SELF_APPROVED_ONLY, NO_CHECKS]


def test_evaluate_compliant_has_no_reasons() -> None:
    out = evaluate(
        prs(),
        reviews(("bob", "APPROVED", "2026-11-16T10:00Z")),
        check_runs((1, "x", "completed", "success")),
        statuses(),
        CFG,
    )
    assert out.iloc[0].is_compliant and out.iloc[0].violation_reasons == []


def test_evaluate_failed_checks_reason() -> None:
    out = evaluate(prs(), reviews(), check_runs((1, "x", "completed", "failure")), statuses(), CFG)
    assert out.iloc[0].violation_reasons == [NO_APPROVAL, FAILED_CHECKS]


def test_evaluate_empty_input() -> None:
    empty = prs().iloc[0:0]
    out = evaluate(empty, reviews(), check_runs(), statuses(), CFG)
    assert out.empty and "is_compliant" in out.columns
