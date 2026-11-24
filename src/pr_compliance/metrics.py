"""Per-task run metrics, logged and written to the ``etl_runs`` table.

The metric names in :class:`RunMetrics` are a contract with the Grafana dashboard
(``etl_runs.metrics->>'api_calls'`` etc.). Keep them stable.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from datetime import UTC, date, datetime
from typing import Any, Protocol

from pr_compliance.logging_config import log_context

logger = logging.getLogger(__name__)


@dataclass
class RunMetrics:
    """Counters for one task instance. Thread-safe increments via :meth:`incr`."""

    api_calls: int = 0
    api_retries: int = 0
    min_rate_limit_remaining: int | None = None
    rate_limit_sleep_s: float = 0.0
    prs_fetched: int = 0
    prs_skipped: int = 0
    records_valid: int = 0
    records_quarantined: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    duration_s: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def incr(self, name: str, amount: float = 1) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + amount)

    def observe_remaining(self, remaining: int) -> None:
        with self._lock:
            if self.min_rate_limit_remaining is None or remaining < self.min_rate_limit_remaining:
                self.min_rate_limit_remaining = remaining

    def as_dict(self) -> dict[str, Any]:
        # Not dataclasses.asdict(): it deep-copies fields and a Lock cannot be copied.
        with self._lock:
            return {f.name: getattr(self, f.name) for f in fields(self) if f.name != "_lock"}


class DbConnection(Protocol):
    """The subset of a DB-API connection we use (psycopg2 connection)."""

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


ConnectionFactory = Callable[[], DbConnection]


@dataclass(frozen=True)
class TaskIdentity:
    run_id: str
    task_id: str
    map_index: int
    run_date: date


class EtlRunRecorder:
    """Writes one ``etl_runs`` row per task attempt. Never raises: monitoring must not
    break the pipeline it monitors."""

    SQL = """
        INSERT INTO etl_runs (run_id, task_id, map_index, run_date, status,
                              started_at, finished_at, metrics, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (run_id, task_id, map_index) DO UPDATE SET
            status = EXCLUDED.status, started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at, metrics = EXCLUDED.metrics,
            error = EXCLUDED.error
    """

    def __init__(self, connection_factory: ConnectionFactory | None) -> None:
        self._factory = connection_factory

    def record(
        self,
        ident: TaskIdentity,
        status: str,
        started_at: datetime,
        metrics: dict[str, Any],
        error: str | None = None,
    ) -> None:
        if self._factory is None:
            return
        try:
            conn = self._factory()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        self.SQL,
                        (
                            ident.run_id,
                            ident.task_id,
                            ident.map_index,
                            ident.run_date,
                            status,
                            started_at,
                            datetime.now(UTC),
                            json.dumps(metrics, default=str),
                            error,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            logger.warning("etl_run_record_failed", exc_info=True)


@contextmanager
def tracked_task(
    ident: TaskIdentity,
    recorder: EtlRunRecorder,
    **log_fields: Any,
) -> Iterator[RunMetrics]:
    """Run a task body with log context, timing, metrics logging and an ``etl_runs`` row.

    Usage::

        with tracked_task(ident, recorder, repo="core") as metrics:
            ...
    """
    metrics = RunMetrics()
    started = datetime.now(UTC)
    t0 = time.monotonic()
    with log_context(
        run_id=ident.run_id,
        task_id=ident.task_id,
        map_index=ident.map_index if ident.map_index >= 0 else None,
        **log_fields,
    ):
        logger.info("task_started")
        try:
            yield metrics
        except Exception as exc:
            metrics.duration_s = round(time.monotonic() - t0, 3)
            logger.exception(
                "task_failed", extra={"error_class": type(exc).__name__, **metrics.as_dict()}
            )
            recorder.record(
                ident, "failed", started, metrics.as_dict(), f"{type(exc).__name__}: {exc}"
            )
            raise
        metrics.duration_s = round(time.monotonic() - t0, 3)
        logger.info("task_succeeded", extra=metrics.as_dict())
        recorder.record(ident, "success", started, metrics.as_dict())
