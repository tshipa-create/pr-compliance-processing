"""Compliance rules. Pure functions over silver DataFrames: no I/O, no mocks needed.

Assumptions (also listed in the README):

* **Code review** - only each reviewer's *latest decisive* review counts
  (APPROVED / CHANGES_REQUESTED / DISMISSED; COMMENTED and PENDING never change the
  approval state, matching GitHub's own behaviour). The PR author's own reviews and
  reviews submitted after the merge are ignored. Passes with >= ``min_approvals``.
* **Status checks** - check runs are de-duplicated per (app, name), keeping the latest
  run (re-runs replace old results). A run passes if it completed with ``success``,
  ``neutral`` or ``skipped``; anything else, including a run still in progress at merge
  time, fails. Legacy commit statuses (latest per context, as the combined-status
  endpoint returns them) must be ``success``. All checks are treated as required because
  branch-protection settings need admin access a public token does not have.
* **No checks at all** fails with reason ``no_checks`` unless ``no_checks_passes``.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

KEY = ["repository", "pr_number"]
DECISIVE_REVIEW_STATES = {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}
PASSING_CONCLUSIONS = {"success", "neutral", "skipped"}

# violation reason codes
NO_APPROVAL = "no_approval"
SELF_APPROVED_ONLY = "self_approved_only"
FAILED_CHECKS = "failed_checks"
NO_CHECKS = "no_checks"


@dataclass(frozen=True)
class RuleConfig:
    min_approvals: int = 1
    no_checks_passes: bool = False


def evaluate_reviews(prs: pd.DataFrame, reviews: pd.DataFrame, cfg: RuleConfig) -> pd.DataFrame:
    """One row per PR: ``approved_review_count``, ``self_approved``, ``code_review_passed``."""
    base = prs[[*KEY, "author", "merged_at"]]
    r = reviews.merge(base, on=KEY, how="inner")
    r = r[r["state"].isin(DECISIVE_REVIEW_STATES)]
    r = r[r["submitted_at"].isna() | (r["submitted_at"] <= r["merged_at"])]

    is_self = r["reviewer"] == r["author"]
    self_approved = (
        r[is_self & (r["state"] == "APPROVED")].groupby(KEY).size().rename("self_approvals")
    )

    others = r[~is_self].sort_values(["submitted_at", "review_id"], na_position="first")
    latest = others.groupby([*KEY, "reviewer"], as_index=False).tail(1)
    approvals = latest[latest["state"] == "APPROVED"].groupby(KEY).size().rename("approvals")

    out = base[KEY].copy()
    out = out.merge(approvals, left_on=KEY, right_index=True, how="left")
    out = out.merge(self_approved, left_on=KEY, right_index=True, how="left")
    out["approved_review_count"] = out["approvals"].fillna(0).astype("int64")
    out["self_approved"] = out["self_approvals"].fillna(0).astype("int64") > 0
    out["code_review_passed"] = out["approved_review_count"] >= cfg.min_approvals
    return out[[*KEY, "approved_review_count", "self_approved", "code_review_passed"]]


def latest_check_runs(check_runs: pd.DataFrame) -> pd.DataFrame:
    """Keep the newest run per (PR, app, check name); re-runs supersede old results."""
    cr = check_runs.copy()
    cr["app_slug"] = cr["app_slug"].fillna("")
    cr = cr.sort_values("check_run_id")
    return cr.groupby([*KEY, "app_slug", "name"], as_index=False).tail(1)


def evaluate_checks(
    prs: pd.DataFrame,
    check_runs: pd.DataFrame,
    statuses: pd.DataFrame,
    cfg: RuleConfig,
) -> pd.DataFrame:
    """One row per PR: ``check_count``, ``failed_check_count``, ``status_checks_passed``."""
    cr = latest_check_runs(check_runs)
    cr_pass = (cr["status"] == "completed") & cr["conclusion"].isin(PASSING_CONCLUSIONS)
    cr_counts = (
        cr.assign(failed=~cr_pass)
        .groupby(KEY)
        .agg(cr_total=("name", "size"), cr_failed=("failed", "sum"))
    )

    # statuses only has rows when the combined endpoint returned total_count > 0, so a
    # 'pending' combined state with zero statuses ("no legacy CI") is ignored naturally.
    st = statuses.dropna(subset=["context"])
    st_counts = (
        st.assign(failed=st["status_state"] != "success")
        .groupby(KEY)
        .agg(st_total=("context", "size"), st_failed=("failed", "sum"))
    )

    out = prs[KEY].copy()
    out = out.merge(cr_counts, left_on=KEY, right_index=True, how="left")
    out = out.merge(st_counts, left_on=KEY, right_index=True, how="left")
    counts = out[["cr_total", "cr_failed", "st_total", "st_failed"]].fillna(0).astype("int64")
    out["check_count"] = counts["cr_total"] + counts["st_total"]
    out["failed_check_count"] = counts["cr_failed"] + counts["st_failed"]
    has_checks = out["check_count"] > 0
    out["status_checks_passed"] = (out["failed_check_count"] == 0) & (
        has_checks | cfg.no_checks_passes
    )
    return out[[*KEY, "check_count", "failed_check_count", "status_checks_passed"]]


def violation_reasons(row: pd.Series) -> list[str]:
    """Reason codes explaining why a PR is non-compliant (empty when compliant)."""
    reasons: list[str] = []
    if not row["code_review_passed"]:
        reasons.append(SELF_APPROVED_ONLY if row["self_approved"] else NO_APPROVAL)
    if not row["status_checks_passed"]:
        reasons.append(NO_CHECKS if row["check_count"] == 0 else FAILED_CHECKS)
    return reasons


def evaluate(
    prs: pd.DataFrame,
    reviews: pd.DataFrame,
    check_runs: pd.DataFrame,
    statuses: pd.DataFrame,
    cfg: RuleConfig,
) -> pd.DataFrame:
    """Apply every rule; returns one row per PR with all rule outputs and reasons."""
    out = prs[KEY].copy()
    out = out.merge(evaluate_reviews(prs, reviews, cfg), on=KEY, how="left")
    out = out.merge(evaluate_checks(prs, check_runs, statuses, cfg), on=KEY, how="left")
    out["is_compliant"] = out["code_review_passed"] & out["status_checks_passed"]
    out["violation_reasons"] = (
        out.apply(violation_reasons, axis=1) if len(out) else pd.Series(dtype="object")
    )
    return out
