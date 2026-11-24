"""PR compliance DAG: GitHub -> bronze -> silver -> gold -> Postgres -> data-quality.

Wiring only. All logic lives in the ``pr_compliance`` package (see ``pipeline.py``).

Runs daily at 00:00 UTC and processes PRs merged during its data interval, i.e. the
previous UTC day ``[data_interval_start, data_interval_end)``. Reruns of the same run
overwrite the same files and upsert the same rows.

    list_repos -> extract[repo] (mapped) -> transform -> load -> data_quality_check
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import (
    Asset,
    Connection,
    CronDataIntervalTimetable,
    TriggerRule,
    dag,
    get_current_context,
    task,
)
from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.sdk.exceptions import AirflowFailException

# Only light modules at parse time: the DAG processor re-parses this file every ~30s.
# pandas/pyarrow/pandera (via pr_compliance.pipeline) are imported inside the tasks.
from pr_compliance.alerts import notify_failure, notify_retry
from pr_compliance.config import Settings
from pr_compliance.exceptions import (
    ConfigurationError,
    DataQualityError,
    GitHubAuthError,
    GitHubResourceError,
)
from pr_compliance.github.models import Window
from pr_compliance.metrics import DbConnection, EtlRunRecorder, TaskIdentity

COMPLIANCE_CONN_ID = "compliance_db"
GITHUB_CONN_ID = "github_api"
COMPLIANCE_TABLE = Asset("postgres://compliance-db:5432/compliance/public/pr_compliance")


def _settings() -> Settings:
    """Settings from env; the GitHub token may instead come from the ``github_api``
    connection (password field) so it is never hard-coded in the DAG."""
    settings = Settings()
    if settings.github_token is None or not settings.github_token.get_secret_value():
        try:
            token = Connection.get(GITHUB_CONN_ID).password
        except Exception:  # connection not defined - GitHubClient raises a clear error
            token = None
        if token:
            settings = settings.model_copy(update={"github_token": token})
    return settings


def _db() -> DbConnection:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn: DbConnection = PostgresHook(postgres_conn_id=COMPLIANCE_CONN_ID).get_conn()
    return conn


def _window(ctx: Any, settings: Settings) -> Window:
    """The run's merge window. Manual triggers have no data interval in Airflow 3, so
    they fall back to the last full UTC day before the run was created."""
    if ctx.get("data_interval_start") is None:
        return Window.previous_day(ctx["dag_run"].run_after, settings.lookback_days)
    return Window.for_interval(
        ctx["data_interval_start"], ctx["data_interval_end"], settings.lookback_days
    )


def _ident(ctx: Any, window: Window) -> TaskIdentity:
    ti = ctx["ti"]
    return TaskIdentity(
        run_id=ctx["run_id"],
        task_id=ti.task_id,
        map_index=ti.map_index if ti.map_index is not None else -1,
        run_date=window.ds,
    )


RECORDER = EtlRunRecorder(_db)

# Errors a retry cannot fix (bad token, missing repo, bad config, failed data-quality
# checks): fail the task now instead of burning Airflow retries. Transient errors were
# already retried inside the GitHub client.
PERMANENT_ERRORS = (ConfigurationError, GitHubAuthError, GitHubResourceError, DataQualityError)


def _no_retry_on_permanent[T](call: Callable[[], T]) -> T:
    try:
        return call()
    except PERMANENT_ERRORS as exc:
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


@dag(
    dag_id="pr_compliance",
    description="Daily GitHub PR code-review compliance for the home-assistant org",
    # 12:00 AM UTC daily. Airflow 3 turns a plain cron string into a CronTriggerTimetable
    # whose data interval has zero length; we need the full previous day, so the
    # interval timetable is explicit.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
        "on_retry_callback": notify_retry,
    },
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=27),  # data for day D must be loaded by 03:00 on D+1
        callback=AsyncCallback("pr_compliance.alerts.notify_deadline_missed"),
    ),
    tags=["compliance", "github"],
    doc_md=__doc__,
)
def pr_compliance() -> None:
    @task
    def list_repos() -> list[str]:
        from pr_compliance import pipeline

        ctx = get_current_context()
        settings = _settings()
        ident = _ident(ctx, _window(ctx, settings))
        return _no_retry_on_permanent(lambda: pipeline.list_repos(settings, ident, RECORDER))

    @task(max_active_tis_per_dag=3, map_index_template="{{ task.op_kwargs['repo'] }}")
    def extract(repo: str) -> str:
        from pr_compliance import pipeline

        ctx = get_current_context()
        settings = _settings()
        window = _window(ctx, settings)
        ident = _ident(ctx, window)
        return _no_retry_on_permanent(
            lambda: pipeline.extract(settings, ident, RECORDER, repo, window)
        )

    # Healthy repos still flow through when some repos failed; the data_quality_check
    # task then fails the run so the gap is alerted on. "min one success" stops a run
    # in which nothing was extracted (e.g. list_repos failed) from loading anything.
    @task(trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS)
    def transform() -> dict[str, str]:
        from pr_compliance import pipeline

        ctx = get_current_context()
        settings = _settings()
        window = _window(ctx, settings)
        return pipeline.transform(settings, _ident(ctx, window), RECORDER, window.ds)

    @task(outlets=[COMPLIANCE_TABLE])
    def load(gold_paths: dict[str, str]) -> dict[str, int]:
        from pr_compliance import pipeline

        ctx = get_current_context()
        settings = _settings()
        window = _window(ctx, settings)
        return pipeline.load(settings, _ident(ctx, window), RECORDER, gold_paths, window.ds, _db)

    @task
    def data_quality_check(repos: list[str], _loaded: dict[str, int]) -> dict[str, bool]:
        from pr_compliance import pipeline

        ctx = get_current_context()
        settings = _settings()
        window = _window(ctx, settings)
        ident = _ident(ctx, window)
        return _no_retry_on_permanent(
            lambda: pipeline.data_quality_check(
                settings, ident, RECORDER, window.ds, repos, window.end, _db
            )
        )

    repos = list_repos()
    extracted = extract.expand(repo=repos)
    gold = transform()
    extracted >> gold
    data_quality_check(repos, load(gold))


pr_compliance()
