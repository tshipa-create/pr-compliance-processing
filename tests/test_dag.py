"""DAG integrity: parses without import errors and has the expected shape.

Needs apache-airflow installed, so it runs inside the Airflow image (``make test-dag``)
and in CI, not on a bare laptop.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow

DAGS = Path(__file__).parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():  # type: ignore[no-untyped-def]
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("pr_compliance")


def test_schedule_is_daily_midnight_utc_with_intervals(dag) -> None:  # type: ignore[no-untyped-def]
    tt = dag.timetable
    assert type(tt).__name__ == "CronDataIntervalTimetable"
    assert tt.summary == "0 0 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_and_dependencies(dag) -> None:  # type: ignore[no-untyped-def]
    assert set(dag.task_ids) == {
        "list_repos",
        "extract",
        "transform",
        "load",
        "data_quality_check",
    }
    t = dag.task_dict
    assert t["extract"].upstream_task_ids == {"list_repos"}
    assert t["transform"].upstream_task_ids == {"extract"}
    assert t["load"].upstream_task_ids == {"transform"}
    assert t["data_quality_check"].upstream_task_ids == {"load", "list_repos"}
    assert t["transform"].trigger_rule == "all_done"


def test_failure_callbacks_and_retries(dag) -> None:  # type: ignore[no-untyped-def]
    for task in dag.tasks:
        assert task.retries == 2
        assert task.on_failure_callback
