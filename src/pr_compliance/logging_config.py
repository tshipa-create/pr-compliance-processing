"""Unified structured (JSON) logging.

Every module logs through ``logging.getLogger(__name__)`` under the ``pr_compliance``
namespace. Messages are short event names (``rate_limit_wait``, ``pr_skipped``) and the
details go in ``extra``. Run context (run_id, task_id, map_index, repo) is attached
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


class JsonFormatter(logging.Formatter):
    """Render a record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(_context.get())
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: int | str = logging.INFO) -> logging.Logger:
    """Attach the JSON handler to the package logger. Safe to call many times.

    The handler writes to stderr, which Airflow captures into the task log; we do not
    propagate to the root logger so lines are not duplicated in Airflow's own format.
    """
    logger = logging.getLogger(ROOT_LOGGER)
    logger.setLevel(level)
    if not any(getattr(h, "_pr_compliance", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        handler._pr_compliance = True  # type: ignore[attr-defined]
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
