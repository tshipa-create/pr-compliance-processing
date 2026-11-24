"""Gold Parquet -> Postgres, through a staging table and an upsert, in one transaction."""

from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from pr_compliance.exceptions import LoadError
from pr_compliance.metrics import ConnectionFactory, RunMetrics

logger = logging.getLogger(__name__)

PR_COLUMNS = [
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
_UPDATABLE = [c for c in PR_COLUMNS if c not in ("repository", "pr_number")]

UPSERT_SQL = f"""
    INSERT INTO pr_compliance ({", ".join(PR_COLUMNS)})
    SELECT {", ".join(PR_COLUMNS)} FROM stg_pr_compliance
    ON CONFLICT (repository, pr_number) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATABLE)},
        loaded_at = now()
    RETURNING (xmax = 0) AS inserted
"""

SUMMARY_COLUMNS = [
    "run_date",
    "repository",
    "total_prs",
    "compliant_prs",
    "compliance_rate",
    "human_prs",
    "human_compliant_prs",
    "human_compliance_rate",
    "violations",
]
NULL = r"\N"


@dataclass(frozen=True)
class LoadResult:
    inserted: int
    updated: int
    summary_rows: int

    def as_xcom(self) -> dict[str, int]:
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "summary_rows": self.summary_rows,
        }


def _pg_array(values: list[str] | None) -> str:
    """Render reason codes as a Postgres array literal. Codes are fixed identifiers."""
    return "{" + ",".join(values or []) + "}"


def gold_to_csv(df: pd.DataFrame) -> io.StringIO:
    out = df[PR_COLUMNS].copy()
    out["violation_reasons"] = out["violation_reasons"].map(
        lambda v: _pg_array(list(v) if v is not None else [])
    )
    out["merged_at"] = pd.to_datetime(out["merged_at"], utc=True).map(lambda t: t.isoformat())
    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False, na_rep=NULL)
    buf.seek(0)
    return buf


class PostgresLoader:
    """Idempotent loader: rerunning the same ``ds`` upserts the same rows."""

    def __init__(self, connection_factory: ConnectionFactory, metrics: RunMetrics | None = None):
        self._factory = connection_factory
        self.metrics = metrics or RunMetrics()

    def load(self, gold_path: Path, summary_path: Path, run_date: date) -> LoadResult:
        gold = pd.read_parquet(gold_path)
        summary = pd.read_parquet(summary_path)
        conn = self._factory()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TEMP TABLE stg_pr_compliance "
                    "(LIKE pr_compliance INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                inserted = updated = 0
                if len(gold):
                    cur.copy_expert(
                        f"COPY stg_pr_compliance ({', '.join(PR_COLUMNS)}) FROM STDIN "
                        f"WITH (FORMAT csv, NULL '{NULL}')",
                        gold_to_csv(gold),
                    )
                    cur.execute(UPSERT_SQL)
                    flags = [row[0] for row in cur.fetchall()]
                    inserted = sum(flags)
                    updated = len(flags) - inserted

                cur.execute("DELETE FROM compliance_summary WHERE run_date = %s", (run_date,))
                for rec in summary.to_dict("records"):
                    cur.execute(
                        f"INSERT INTO compliance_summary ({', '.join(SUMMARY_COLUMNS)}) "
                        f"VALUES ({', '.join(['%s'] * len(SUMMARY_COLUMNS))})",
                        [
                            run_date,
                            rec["repository"],
                            int(rec["total_prs"]),
                            int(rec["compliant_prs"]),
                            _nullable_float(rec["compliance_rate"]),
                            int(rec["human_prs"]),
                            int(rec["human_compliant_prs"]),
                            _nullable_float(rec["human_compliance_rate"]),
                            json.dumps(json.loads(rec["violations"])),
                        ],
                    )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.exception("load_rolled_back", extra={"rows": len(gold)})
            raise LoadError(f"load failed and was rolled back: {exc}") from exc
        finally:
            conn.close()

        self.metrics.incr("rows_inserted", inserted)
        self.metrics.incr("rows_updated", updated)
        result = LoadResult(inserted, updated, len(summary))
        logger.info("load_committed", extra=result.as_xcom())
        return result


def _nullable_float(value: object) -> float | None:
    if value is None or pd.isna(value):  # type: ignore[call-overload]
        return None
    return float(value)  # type: ignore[arg-type]
