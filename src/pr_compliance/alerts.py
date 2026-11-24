"""Failure alerting. The only place alerts are sent from.

``notify_failure`` is the Airflow ``on_failure_callback``. It runs once per task after its
retries are exhausted and:

1. logs a structured ``pipeline_alert`` line (always),
2. sends an e-mail through the ``smtp_alerts`` Airflow connection (Mailpit locally).

Delivery problems are logged as ``alert_delivery_failed`` and never raised: a broken
alert channel must not hide the original error. Switching to Slack means replacing
:func:`send_email` with a Slack notifier call; nothing else changes.
"""

from __future__ import annotations

import asyncio
import html
import logging
from collections.abc import Callable, Mapping
from typing import Any

from pr_compliance.config import get_settings
from pr_compliance.logging_config import configure_logging

logger = logging.getLogger(__name__)

SMTP_CONN_ID = "smtp_alerts"

Sender = Callable[[str, str, str], None]


def send_email(to: str, subject: str, body_html: str) -> None:
    """Send via Airflow's SMTP provider hook (imported lazily: Airflow-only dependency)."""
    from airflow.providers.smtp.hooks.smtp import SmtpHook  # noqa: PLC0415 - Airflow-only dep

    settings = get_settings()
    with SmtpHook(smtp_conn_id=SMTP_CONN_ID) as hook:
        hook.send_email_smtp(
            to=to,
            subject=subject,
            html_content=body_html,
            from_email=settings.alert_email_from,
        )


def build_alert(context: Mapping[str, Any]) -> dict[str, Any]:
    """Extract the fields an on-call engineer needs from an Airflow callback context."""
    ti = context.get("ti") or context.get("task_instance")
    exc = context.get("exception")
    dag_run = context.get("dag_run")
    logical_date = context.get("logical_date")
    return {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "map_index": getattr(ti, "map_index", -1),
        "map_label": getattr(ti, "rendered_map_index", None),  # e.g. the repo name
        "try_number": getattr(ti, "try_number", None),
        "run_id": getattr(dag_run, "run_id", None) or getattr(ti, "run_id", None),
        "logical_date": str(logical_date) if logical_date else None,
        "error_class": type(exc).__name__ if exc else None,
        "error": str(exc)[:2000] if exc else None,
        "log_url": getattr(ti, "log_url", None),
    }


def format_email(alert: Mapping[str, Any]) -> tuple[str, str]:
    label = alert.get("map_label") or (
        None if alert["map_index"] in (-1, None) else alert["map_index"]
    )
    subject = (
        f"[pr_compliance] FAILED {alert['task_id']}"
        f"{'' if label is None else f'[{label}]'} - {alert['error_class']}"
    )
    rows = "".join(
        f"<tr><th align='left'>{html.escape(k)}</th><td>{html.escape(str(v))}</td></tr>"
        for k, v in alert.items()
        if v is not None
    )
    return subject, f"<h3>Airflow task failed</h3><table>{rows}</table>"


def notify_failure(context: Mapping[str, Any], sender: Sender | None = None) -> None:
    """``on_failure_callback``: log + e-mail. Never raises."""
    configure_logging()
    alert = build_alert(context)
    logger.error("pipeline_alert", extra=alert)
    try:
        subject, body = format_email(alert)
        (sender or send_email)(get_settings().alert_email_to, subject, body)
        logger.info("alert_delivered", extra={"channel": "email", "task_id": alert["task_id"]})
    except Exception:
        logger.exception("alert_delivery_failed", extra={"task_id": alert["task_id"]})


def notify_retry(context: Mapping[str, Any]) -> None:
    """``on_retry_callback``: a warning in the log only; retries are expected."""
    alert = build_alert(context)
    logger.warning("task_retrying", extra=alert)


async def notify_deadline_missed(**kwargs: Any) -> None:
    """Deadline-alert callback (runs in the triggerer): the run is late, which also covers
    a stuck or dead scheduler that would otherwise fail nothing."""
    configure_logging()
    context = kwargs.get("context") or {}
    dag_run = context.get("dag_run") if isinstance(context, Mapping) else None
    alert = {
        "dag_id": "pr_compliance",
        "task_id": "deadline",
        "map_index": -1,
        "run_id": getattr(dag_run, "run_id", None) or kwargs.get("run_id"),
        "error_class": "DeadlineMissed",
        "error": "DAG run did not finish before its deadline (03:00 UTC)",
    }
    logger.error("pipeline_alert", extra=alert)
    try:
        subject, body = format_email(alert)
        await asyncio.to_thread(send_email, get_settings().alert_email_to, subject, body)
    except Exception:
        logger.exception("alert_delivery_failed", extra={"task_id": "deadline"})
