"""Task entry points called by the Airflow DAG.

Each function takes plain values (settings, run identity, a DB connection factory) and
returns small JSON-serialisable values (paths, counts) for XCom. They contain no Airflow
imports, so the whole pipeline can be run and tested without Airflow.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from pr_compliance.config import Settings
from pr_compliance.extract.extractor import PRExtractor
from pr_compliance.github.client import GitHubClient
from pr_compliance.github.models import Window
from pr_compliance.load.loader import PostgresLoader
from pr_compliance.logging_config import configure_logging
from pr_compliance.metrics import ConnectionFactory, EtlRunRecorder, TaskIdentity, tracked_task
from pr_compliance.quality.checks import DataQualityChecker
from pr_compliance.storage import Storage
from pr_compliance.transform.gold import GoldBuilder
from pr_compliance.transform.rules import RuleConfig
from pr_compliance.transform.silver import SilverBuilder


def list_repos(settings: Settings, ident: TaskIdentity, recorder: EtlRunRecorder) -> list[str]:
    configure_logging()
    with tracked_task(ident, recorder) as metrics:
        client = GitHubClient(settings, metrics)
        return PRExtractor(client, Storage(settings.data_dir), settings).list_repos()


def extract(
    settings: Settings,
    ident: TaskIdentity,
    recorder: EtlRunRecorder,
    repo: str,
    window: Window,
) -> str:
    configure_logging()
    with tracked_task(ident, recorder, repo=repo) as metrics:
        client = GitHubClient(settings, metrics)
        extractor = PRExtractor(client, Storage(settings.data_dir), settings)
        return str(extractor.extract_repo(repo, window, ident.run_id))


def transform(
    settings: Settings, ident: TaskIdentity, recorder: EtlRunRecorder, ds: date
) -> dict[str, str]:
    configure_logging()
    with tracked_task(ident, recorder) as metrics:
        storage = Storage(settings.data_dir)
        silver = SilverBuilder(storage, metrics).build(ds, ident.run_id)
        cfg = RuleConfig(settings.min_approvals, settings.no_checks_passes)
        gold = GoldBuilder(storage, cfg, settings.exclude_bots).build(silver.frames, ds)
        return gold.as_xcom()


def load(
    settings: Settings,
    ident: TaskIdentity,
    recorder: EtlRunRecorder,
    gold_paths: dict[str, str],
    ds: date,
    connection_factory: ConnectionFactory,
) -> dict[str, int]:
    configure_logging()
    with tracked_task(ident, recorder) as metrics:
        loader = PostgresLoader(connection_factory, metrics)
        result = loader.load(Path(gold_paths["pr_compliance"]), Path(gold_paths["summary"]), ds)
        return result.as_xcom()


def data_quality_check(
    settings: Settings,
    ident: TaskIdentity,
    recorder: EtlRunRecorder,
    ds: date,
    expected_repos: list[str],
    window_end: datetime,
    connection_factory: ConnectionFactory,
) -> dict[str, bool]:
    configure_logging()
    with tracked_task(ident, recorder):
        checker = DataQualityChecker(settings, Storage(settings.data_dir), connection_factory)
        results = checker.run(ds, ident.run_id, expected_repos, window_end)
        return {r.name: r.passed for r in results}
