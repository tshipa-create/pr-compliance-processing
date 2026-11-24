"""Logging format, run metrics recording and alert behaviour."""

from __future__ import annotations

import io
import json
import logging
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from pr_compliance import alerts
from pr_compliance.logging_config import JsonFormatter, log_context
from pr_compliance.metrics import EtlRunRecorder, RunMetrics, TaskIdentity, tracked_task

IDENT = TaskIdentity(
    run_id="scheduled__x", task_id="extract", map_index=0, run_date=date(2026, 11, 1)
)


@pytest.fixture
def captured() -> Any:
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("pr_compliance")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    yield lambda: [json.loads(line) for line in buf.getvalue().splitlines()]
    logger.removeHandler(handler)


def test_json_lines_carry_context_and_extras(captured: Any) -> None:
    log = logging.getLogger("pr_compliance.test")
    with log_context(run_id="r1", repo="core"):
        log.warning("rate_limit_wait", extra={"remaining": 42, "sleep_s": 318})
    line = captured()[-1]
    assert line["event"] == "rate_limit_wait"
    assert line["level"] == "WARNING"
    assert line["run_id"] == "r1" and line["repo"] == "core"
    assert line["remaining"] == 42 and line["sleep_s"] == 318
    assert line["timestamp"].endswith("+00:00")


def test_exceptions_are_serialised(captured: Any) -> None:
    log = logging.getLogger("pr_compliance.test")
    try:
        raise ValueError("bad")
    except ValueError:
        log.exception("boom")
    line = captured()[-1]
    assert line["exc_type"] == "ValueError" and "bad" in line["exc"]


class FakeCursor:
    def __init__(self, sink: list[Any]) -> None:
        self.sink = sink

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *a: object) -> None: ...

    def execute(self, sql: str, params: Any = None) -> None:
        self.sink.append(params)


class FakeConn:
    def __init__(self, sink: list[Any]) -> None:
        self.sink = sink

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.sink)

    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


def test_tracked_task_records_success_and_metric_names() -> None:
    rows: list[Any] = []
    recorder = EtlRunRecorder(lambda: FakeConn(rows))
    with tracked_task(IDENT, recorder) as m:
        m.incr("api_calls", 3)
    run_id, task_id, map_index, _, status, *_rest, metrics_json, error = rows[0]
    metrics = json.loads(metrics_json)
    assert (run_id, task_id, map_index, status, error) == (
        "scheduled__x",
        "extract",
        0,
        "success",
        None,
    )
    # contract with the Grafana dashboard
    for key in ("api_calls", "min_rate_limit_remaining", "records_quarantined", "duration_s"):
        assert key in metrics
    assert metrics["api_calls"] == 3


def test_tracked_task_records_failure_and_reraises() -> None:
    rows: list[Any] = []
    with pytest.raises(RuntimeError), tracked_task(IDENT, EtlRunRecorder(lambda: FakeConn(rows))):
        raise RuntimeError("nope")
    assert rows[0][4] == "failed" and "RuntimeError: nope" in rows[0][-1]


def test_recorder_never_raises() -> None:
    def broken() -> Any:
        raise ConnectionError("db down")

    EtlRunRecorder(broken).record(IDENT, "success", None, {})  # type: ignore[arg-type]


def test_metrics_thread_safe_min() -> None:
    m = RunMetrics()
    for v in (50, 10, 30):
        m.observe_remaining(v)
    assert m.min_rate_limit_remaining == 10


def _context() -> dict[str, Any]:
    ti = SimpleNamespace(
        dag_id="pr_compliance",
        task_id="extract",
        map_index=2,
        try_number=3,
        run_id="r",
        log_url="http://localhost:8080/log",
    )
    return {
        "ti": ti,
        "exception": ValueError("token=secret? no: boom"),
        "logical_date": "2026-11-01",
    }


def test_notify_failure_sends_email() -> None:
    sent: list[tuple[str, str, str]] = []
    alerts.notify_failure(_context(), sender=lambda to, s, b: sent.append((to, s, b)))
    _to, subject, body = sent[0]
    assert subject == "[pr_compliance] FAILED extract[2] - ValueError"
    assert "http://localhost:8080/log" in body


def test_notify_failure_swallows_delivery_errors(caplog: pytest.LogCaptureFixture) -> None:
    def broken(*_: str) -> None:
        raise OSError("smtp down")

    alerts.notify_failure(_context(), sender=broken)  # must not raise


def test_structlog_bridge_keeps_level_and_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    structlog = pytest.importorskip("structlog")
    from pr_compliance import logging_config

    events: list[dict[str, Any]] = []

    def capture(_: Any, method: str, event_dict: dict[str, Any]) -> Any:
        events.append({"method": method, **event_dict})
        raise structlog.DropEvent

    structlog.configure(processors=[capture])
    try:
        logger = logging_config.configure_logging()
        assert isinstance(logger.handlers[-1], logging_config.StructlogBridgeHandler)
        with log_context(run_id="r9"):
            logging.getLogger("pr_compliance.x").info("pr_skipped", extra={"pr_number": 7})
    finally:
        structlog.reset_defaults()
        logging_config.configure_logging()
    assert events[-1]["method"] == "info"
    assert events[-1]["event"] == "pr_skipped"
    assert events[-1]["pr_number"] == 7 and events[-1]["run_id"] == "r9"
