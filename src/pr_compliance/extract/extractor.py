"""Extract: GitHub -> bronze JSON, one partition per (ds, repo)."""

from __future__ import annotations

import contextvars
import logging
import shutil
import tempfile
import time
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pr_compliance.config import Settings
from pr_compliance.exceptions import GitHubResourceError
from pr_compliance.github.client import GitHubClient
from pr_compliance.github.models import (
    CHECK_RUNS,
    COMMITS,
    COMMITS_LIST_CAP,
    MANIFEST,
    PULL_REQUESTS,
    REVIEWS,
    STATUSES,
    Manifest,
    RawRecord,
    SkippedPR,
    Window,
)
from pr_compliance.storage import Storage

logger = logging.getLogger(__name__)


def parse_ts(value: str | None) -> datetime | None:
    """Parse GitHub's ISO-8601 ``...Z`` timestamps into aware UTC datetimes."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


@dataclass
class PRDetails:
    """Everything fetched for one PR besides the PR object itself."""

    number: int
    reviews: list[RawRecord] = field(default_factory=list)
    commits: list[RawRecord] = field(default_factory=list)
    status: RawRecord | None = None
    check_runs: list[RawRecord] = field(default_factory=list)
    skipped: list[SkippedPR] = field(default_factory=list)


class PRExtractor:
    """Lists repos, finds PRs merged inside a window and fetches their details."""

    def __init__(self, client: GitHubClient, storage: Storage, settings: Settings) -> None:
        self.client = client
        self.storage = storage
        self.settings = settings
        self.org = settings.github_org

    # ---- repos ---------------------------------------------------------------------------
    def list_repos(self) -> list[str]:
        """Public, non-archived, non-empty repos of the org (or the allowlist)."""
        if self.settings.repo_allowlist:
            logger.info("repos_from_allowlist", extra={"repos": self.settings.repo_allowlist})
            return sorted(self.settings.repo_allowlist)
        repos: list[str] = []
        for repo in self.client.paginate(f"/orgs/{self.org}/repos", {"type": "public"}):
            if repo.get("archived") or repo.get("disabled"):
                continue
            if repo.get("size", 1) == 0:
                logger.info("repo_skipped_empty", extra={"repo": repo["name"]})
                continue
            repos.append(repo["name"])
        logger.info("repos_listed", extra={"repo_count": len(repos)})
        return sorted(repos)

    # ---- PRs -----------------------------------------------------------------------------
    def iter_merged_prs(self, repo: str, window: Window) -> Iterator[RawRecord]:
        """Yield PRs merged inside ``window``.

        The closed-PR list is sorted by ``updated`` descending. A PR's ``updated_at`` is
        always >= its ``merged_at``, so once ``updated_at < window.start`` no later page
        can contain a PR merged inside the window and we stop paginating.
        """
        params = {"state": "closed", "sort": "updated", "direction": "desc"}
        for pr in self.client.paginate(f"/repos/{self.org}/{repo}/pulls", params):
            updated_at = parse_ts(pr.get("updated_at"))
            if updated_at is not None and updated_at < window.start:
                return
            merged_at = parse_ts(pr.get("merged_at"))
            if merged_at is None:  # closed without merge
                continue
            if window.contains(merged_at):
                yield pr

    def fetch_pr_details(self, repo: str, pr: RawRecord) -> PRDetails:
        """Fetch reviews, commits, combined status and check runs for one PR.

        A 404/409/422 on one resource is recorded as a skip and the rest still runs;
        anything else (auth, exhausted retries) propagates and fails the repo.
        """
        number = int(pr["number"])
        head_sha = pr["head"]["sha"]  # checks run on the PR's last commit, not the merge
        base = f"/repos/{self.org}/{repo}"
        details = PRDetails(number=number)

        def _skip(resource: str, exc: GitHubResourceError) -> None:
            details.skipped.append(
                {"number": number, "resource": resource, "status": exc.status, "reason": str(exc)}
            )
            logger.warning(
                "pr_resource_skipped",
                extra={"pr_number": number, "resource": resource, "status": exc.status},
            )

        try:
            details.reviews = list(self.client.paginate(f"{base}/pulls/{number}/reviews"))
        except GitHubResourceError as exc:
            _skip(REVIEWS, exc)
        try:
            details.commits = list(self.client.paginate(f"{base}/pulls/{number}/commits"))
        except GitHubResourceError as exc:
            _skip(COMMITS, exc)
        try:
            details.status = self.client.get_json(f"{base}/commits/{head_sha}/status")
        except GitHubResourceError as exc:
            _skip(STATUSES, exc)
        try:
            details.check_runs = list(
                self.client.paginate(
                    f"{base}/commits/{head_sha}/check-runs", items_key="check_runs"
                )
            )
        except GitHubResourceError as exc:
            _skip(CHECK_RUNS, exc)
        return details

    def extract_repo(self, repo: str, window: Window, run_id: str) -> Path:
        """Write the bronze partition for ``repo`` and return its path.

        Files are written into a temp directory and swapped in at the end, so a crash
        leaves the previous partition (if any) intact and a rerun replaces it whole.
        """
        t0 = time.monotonic()
        calls_before = self.client.metrics.api_calls
        final_dir = self.storage.bronze_repo(window.ds, repo)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{repo}.", dir=final_dir.parent))
        try:
            status = "success"
            try:
                prs, max_hit = self._collect_prs(repo, window)
            except GitHubResourceError as exc:
                if exc.status != 409:
                    raise
                logger.warning("repo_skipped_empty", extra={"repo": repo})  # 409 = empty repo
                prs, max_hit, status = [], False, "skipped_empty_repo"

            details = self._fetch_all(repo, prs)
            manifest = self._write_partition(
                staging, repo, window, run_id, status, prs, details, max_hit
            )
            manifest["api_calls"] = self.client.metrics.api_calls - calls_before
            manifest["duration_s"] = round(time.monotonic() - t0, 3)
            self.storage.write_json(staging / MANIFEST, manifest)
            self.storage.replace_dir(staging, final_dir)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

        self.client.metrics.incr("prs_fetched", len(prs))
        self.client.metrics.incr("prs_skipped", len({s["number"] for s in manifest["skipped_prs"]}))
        logger.info(
            "repo_extracted",
            extra={
                "repo": repo,
                "pr_count": len(prs),
                "skipped": len(manifest["skipped_prs"]),
                "api_calls": manifest["api_calls"],
                "duration_s": manifest["duration_s"],
                "path": str(final_dir),
            },
        )
        return final_dir

    # ---- helpers -------------------------------------------------------------------------
    def _collect_prs(self, repo: str, window: Window) -> tuple[list[RawRecord], bool]:
        prs: list[RawRecord] = []
        for pr in self.iter_merged_prs(repo, window):
            if len(prs) >= self.settings.max_prs_per_repo:
                logger.warning(
                    "max_prs_per_repo_hit",
                    extra={"repo": repo, "limit": self.settings.max_prs_per_repo},
                )
                return prs, True
            prs.append(pr)
        return prs, False

    def _fetch_all(self, repo: str, prs: list[RawRecord]) -> dict[int, PRDetails]:
        """Fan the per-PR fetches out over a thread pool (bonus: concurrency)."""
        results: dict[int, PRDetails] = {}
        if not prs:
            return results
        workers = min(self.settings.extract_max_workers, len(prs))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"gh-{repo}") as pool:
            futures: dict[Future[PRDetails], int] = {}
            for pr in prs:
                # Copy the logging ContextVars into the worker thread.
                ctx = contextvars.copy_context()
                fut = pool.submit(ctx.run, self.fetch_pr_details, repo, pr)
                futures[fut] = int(pr["number"])
            try:
                for fut in as_completed(futures):
                    details = fut.result()  # re-raises fatal errors (auth, retries exhausted)
                    results[details.number] = details
            except BaseException:
                for pending in futures:
                    pending.cancel()  # don't keep calling GitHub for a repo that has failed
                raise
        return results

    def _write_partition(
        self,
        out: Path,
        repo: str,
        window: Window,
        run_id: str,
        status: str,
        prs: list[RawRecord],
        details: dict[int, PRDetails],
        max_hit: bool,
    ) -> Manifest:
        by_pr: dict[str, dict[str, Any]] = {REVIEWS: {}, COMMITS: {}, STATUSES: {}, CHECK_RUNS: {}}
        skipped: list[SkippedPR] = []
        truncated: list[int] = []
        for number, d in sorted(details.items()):
            key = str(number)
            by_pr[REVIEWS][key] = d.reviews
            by_pr[COMMITS][key] = d.commits
            by_pr[CHECK_RUNS][key] = d.check_runs
            if d.status is not None:
                by_pr[STATUSES][key] = d.status
            skipped.extend(d.skipped)
            if len(d.commits) >= COMMITS_LIST_CAP:
                truncated.append(number)

        self.storage.write_json(out / f"{PULL_REQUESTS}.json", prs)
        for entity, payload in by_pr.items():
            self.storage.write_json(out / f"{entity}.json", payload)

        return {
            "repo": repo,
            "organization": self.org,
            "ds": window.ds.isoformat(),
            "run_id": run_id,
            "status": status,
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "pr_count": len(prs),
            "skipped_prs": skipped,
            "commits_truncated": truncated,
            "max_prs_hit": max_hit,
            "api_calls": 0,
            "duration_s": 0.0,
            "extracted_at": datetime.now(UTC).isoformat(),
        }
