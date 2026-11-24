"""Unified structured (JSON) logging.

Every module logs through ``logging.getLogger(__name__)`` under the ``pr_compliance``
namespace (plain stdlib logging, so the library has no Airflow dependency). Messages
are short event names (``rate_limit_wait``, ``pr_skipped``) and the details go in
``extra``. Run context (run_id, task_id, map_index, repo) is attached
automatically from a ``ContextVar`` set by :func:`log_context`.

Example line::

    {"timestamp": "2026-11-25T00:03:12.120Z", "level": "WARNING", "logger":
     "pr_compliance.github.rate_limiter", "event": "rate_limit_wait", "run_id": "...",
     "task_id": "extract", "map_index": 0, "repo": "core", "remaining": 42, "sleep_s": 318}
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

ROOT_LOGGER = "pr_compliance"

_context: ContextVar[dict[str, Any]] = ContextVar("pr_compliance_log_context", default={})  # noqa: B039

# Attributes every LogRecord has; anything else on the record came from ``extra=``.
_RESERVED = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


def record_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Run context plus the ``extra=`` fields of a record."""
    fields = dict(_context.get())
    for key, value in record.__dict__.items():
        if key not in _RESERVED and not key.startswith("_"):
            fields[key] = value
    return fields


class JsonFormatter(logging.Formatter):
    """Render a record as one JSON object per line (used outside Airflow)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            **record_fields(record),
        }
        if record.exc_info:
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StructlogBridgeHandler(logging.Handler):
    """Forward records into Airflow 3's structlog pipeline (inside a task).

    Airflow writes task logs as JSON lines through structlog. Its bridge for plain
    stdlib records drops ``extra`` fields, and anything printed to stderr is re-tagged
    as ``task.stderr`` at ERROR level. Re-emitting through structlog with the fields as
    key/values keeps the right level and makes every field a first-class JSON key that
    the Airflow UI renders.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            import structlog  # noqa: PLC0415 - only present alongside Airflow

            log = structlog.get_logger(record.name)
            method = getattr(log, record.levelname.lower(), log.info)
            kwargs = record_fields(record)
            if record.exc_info:
                kwargs["exc_info"] = record.exc_info
            method(record.getMessage(), **kwargs)
        except Exception:  # pragma: no cover - logging must never break the task
            self.handleError(record)


def _structlog_configured() -> bool:
    try:
        import structlog  # noqa: PLC0415
    except ImportError:
        return False
    return bool(structlog.is_configured())


def configure_logging(level: int | str = logging.INFO) -> logging.Logger:
    """Attach exactly one handler to the package logger. Safe to call many times.

    * Inside an Airflow task (structlog configured by Airflow): :class:`StructlogBridgeHandler`.
    * Anywhere else (tests, CLI): :class:`JsonFormatter` on stderr.

    The package logger does not propagate, so lines are never duplicated.
    """
    logger = logging.getLogger(ROOT_LOGGER)
    logger.setLevel(level)
    for existing in [h for h in logger.handlers if getattr(h, "_pr_compliance", False)]:
        logger.removeHandler(existing)
    handler: logging.Handler
    if _structlog_configured():
        handler = StructlogBridgeHandler()
    else:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
    setattr(handler, "_pr_compliance", True)  # noqa: B010 - marker attribute
    logger.addHandler(handler)
    logger.propagate = False
    return logger


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Add fields to every log line emitted inside the block (thread-safe, nestable).

    Worker threads do not inherit ContextVars automatically; the extractor copies the
    context into its thread pool with ``contextvars.copy_context()``.
    """
    token = _context.set({**_context.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    """Return a copy of the active log context (used by alerts)."""
    return dict(_context.get())
