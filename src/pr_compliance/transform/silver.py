"""Bronze -> silver: validate every raw record against its JSON Schema, quarantine the
bad ones, flatten the good ones into typed DataFrames and write one Parquet per entity."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from pr_compliance.github.models import (
    CHECK_RUNS,
    COMMITS,
    MANIFEST,
    PULL_REQUESTS,
    REVIEWS,
    STATUSES,
    Manifest,
)
from pr_compliance.metrics import RunMetrics
from pr_compliance.storage import Storage
from pr_compliance.transform.schema_loader import EntitySchema, load_schema

logger = logging.getLogger(__name__)

GHOST_LOGIN = "ghost"  # what GitHub shows for deleted accounts
USABLE_MANIFEST_STATUSES = {"success", "skipped_empty_repo"}

# silver column -> dotted path in the raw record. dtypes come from the JSON Schema.
COLUMN_SPECS: dict[str, dict[str, str]] = {
    PULL_REQUESTS: {
        "pr_number": "number",
        "pr_title": "title",
        "state": "state",
        "merged_at": "merged_at",
        "updated_at": "updated_at",
        "html_url": "html_url",
        "author": "user.login",
        "author_id": "user.id",
        "author_type": "user.type",
        "base_ref": "base.ref",
        "head_ref": "head.ref",
        "head_sha": "head.sha",
    },
    REVIEWS: {
        "review_id": "id",
        "state": "state",
        "submitted_at": "submitted_at",
        "reviewer": "user.login",
        "reviewer_id": "user.id",
        "reviewer_type": "user.type",
    },
    COMMITS: {
        "sha": "sha",
        "author_name": "commit.author.name",
        "authored_at": "commit.author.date",
        "author_login": "author.login",
    },
    STATUSES: {
        # one row per legacy status context, plus the combined state of the commit
        "combined_state": "state",
        "total_count": "total_count",
        "context": "statuses.context",
        "status_state": "statuses.state",
        "status_updated_at": "statuses.updated_at",
    },
    CHECK_RUNS: {
        "check_run_id": "id",
        "name": "name",
        "status": "status",
        "conclusion": "conclusion",
        "started_at": "started_at",
        "completed_at": "completed_at",
        "app_slug": "app.slug",
    },
}
CONTEXT_DTYPES = {"organization": "string", "repository": "string", "pr_number": "Int64"}


@dataclass
class SilverResult:
    paths: dict[str, Path]
    stats: dict[str, Any]
    frames: dict[str, pd.DataFrame] = field(repr=False, default_factory=dict)


def get_path(record: Any, dotted: str) -> Any:
    """Safe nested lookup: ``get_path(pr, "user.login")`` -> None if any level is None."""
    node = record
    for part in dotted.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


class SilverBuilder:
    """Turns one ``ds`` of bronze partitions into validated, typed silver Parquet."""

    def __init__(self, storage: Storage, metrics: RunMetrics | None = None) -> None:
        self.storage = storage
        self.metrics = metrics or RunMetrics()

    def build(self, ds: date) -> SilverResult:
        manifests = self.read_manifests(ds)
        rows: dict[str, list[dict[str, Any]]] = {e: [] for e in COLUMN_SPECS}
        quarantined: dict[str, list[dict[str, Any]]] = {e: [] for e in COLUMN_SPECS}
        counts = {e: {"valid": 0, "quarantined": 0} for e in COLUMN_SPECS}

        for repo_dir, manifest in manifests:
            ctx_base = {"organization": manifest["organization"], "repository": manifest["repo"]}
            truncated = set(manifest["commits_truncated"])
            for entity, pr_number, raw in self._iter_raw(repo_dir):
                schema = load_schema(entity)
                errors = schema.errors(raw)
                ctx = {**ctx_base, "pr_number": pr_number}
                if errors:
                    counts[entity]["quarantined"] += 1
                    quarantined[entity].append({**ctx, "errors": errors, "record": raw})
                    logger.warning(
                        "record_quarantined",
                        extra={"entity": entity, **ctx, "errors": errors[:3]},
                    )
                    continue
                counts[entity]["valid"] += 1
                for flat in self._flatten(entity, raw):
                    row = {**ctx, **flat}
                    if entity == PULL_REQUESTS:
                        row["pr_number"] = flat["pr_number"]
                        row["commits_truncated"] = flat["pr_number"] in truncated
                    rows[entity].append(row)

        out_dir = self.storage.reset_dir(self.storage.silver_ds(ds))
        q_dir = self.storage.quarantine_ds(ds)
        paths: dict[str, Path] = {}
        frames: dict[str, pd.DataFrame] = {}
        for entity, entity_rows in rows.items():
            df = self._to_frame(load_schema(entity), entity, entity_rows)
            frames[entity] = df
            paths[entity] = self.storage.write_parquet(out_dir / f"{entity}.parquet", df)
            if quarantined[entity]:
                self.storage.write_jsonl(q_dir / f"{entity}.jsonl", quarantined[entity])

        total_valid = sum(c["valid"] for c in counts.values())
        total_q = sum(c["quarantined"] for c in counts.values())
        self.metrics.incr("records_valid", total_valid)
        self.metrics.incr("records_quarantined", total_q)
        stats = {
            "ds": ds.isoformat(),
            "repos": [m["repo"] for _, m in manifests],
            "entities": counts,
            "records_valid": total_valid,
            "records_quarantined": total_q,
            "quarantine_rate": round(total_q / (total_valid + total_q), 6)
            if (total_valid + total_q)
            else 0.0,
        }
        self.storage.write_json(out_dir / "_stats.json", stats)
        logger.info("silver_written", extra={k: v for k, v in stats.items() if k != "repos"})
        return SilverResult(paths=paths, stats=stats, frames=frames)

    # ---- helpers -------------------------------------------------------------------------
    def read_manifests(self, ds: date) -> list[tuple[Path, Manifest]]:
        """Usable bronze partitions for ``ds`` (sorted by repo for determinism)."""
        found: list[tuple[Path, Manifest]] = []
        for repo_dir in sorted(self.storage.bronze_ds(ds).glob("repo=*")):
            manifest_path = repo_dir / MANIFEST
            if not manifest_path.exists():
                logger.warning("bronze_partition_without_manifest", extra={"path": str(repo_dir)})
                continue
            manifest: Manifest = self.storage.read_json(manifest_path)
            if manifest["status"] in USABLE_MANIFEST_STATUSES:
                found.append((repo_dir, manifest))
        return found

    def _iter_raw(self, repo_dir: Path) -> Iterator[tuple[str, int | None, Any]]:
        """Yield ``(entity, pr_number, raw_record)`` for every record in a partition."""
        for pr in self.storage.read_json(repo_dir / f"{PULL_REQUESTS}.json"):
            yield PULL_REQUESTS, pr.get("number"), pr
        for entity in (REVIEWS, COMMITS, CHECK_RUNS):
            for pr_number, items in self.storage.read_json(repo_dir / f"{entity}.json").items():
                for item in items:
                    yield entity, int(pr_number), item
        for pr_number, status in self.storage.read_json(repo_dir / f"{STATUSES}.json").items():
            yield STATUSES, int(pr_number), status

    @staticmethod
    def _flatten(entity: str, raw: dict[str, Any]) -> list[dict[str, Any]]:
        spec = COLUMN_SPECS[entity]
        if entity == STATUSES:
            top = {c: get_path(raw, p) for c, p in spec.items() if not p.startswith("statuses.")}
            return [
                {
                    **top,
                    **{
                        c: get_path(item, p.removeprefix("statuses."))
                        for c, p in spec.items()
                        if p.startswith("statuses.")
                    },
                }
                for item in raw.get("statuses") or []
            ]
        flat = {col: get_path(raw, path) for col, path in spec.items()}
        if entity == PULL_REQUESTS and flat["author"] is None:
            flat["author"] = GHOST_LOGIN  # deleted account: keep the PR
        if entity == REVIEWS and flat["reviewer"] is None:
            flat["reviewer"] = GHOST_LOGIN
        return [flat]

    @staticmethod
    def _to_frame(schema: EntitySchema, entity: str, rows: list[dict[str, Any]]) -> pd.DataFrame:
        dtypes = {**CONTEXT_DTYPES}
        dtypes.update({col: schema.dtype_for(path) for col, path in COLUMN_SPECS[entity].items()})
        if entity == PULL_REQUESTS:
            dtypes["commits_truncated"] = "boolean"
        df = pd.DataFrame(rows, columns=list(dtypes))
        for col, dtype in dtypes.items():
            if dtype.startswith("datetime64"):
                df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
            else:
                df[col] = df[col].astype(dtype)  # type: ignore[call-overload]
        return df
