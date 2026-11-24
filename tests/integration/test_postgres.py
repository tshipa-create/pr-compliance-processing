"""Loader and data-quality checks against a real Postgres.

Run with ``COMPLIANCE_DB_URL=postgresql://user:pw@host:port/db pytest -m integration``.
The DDL in ``sql/init`` is applied to a fresh schema per test.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pr_compliance.config import Settings
from pr_compliance.exceptions import DataQualityError, LoadError
from pr_compliance.load.loader import PostgresLoader
from pr_compliance.quality.checks import DataQualityChecker
from pr_compliance.storage import Storage
from pr_compliance.transform.gold import GoldBuilder
from pr_compliance.transform.rules import RuleConfig
from pr_compliance.transform.silver import SilverBuilder
from tests.unit.test_transform import DS, pick, write_bronze

psycopg2 = pytest.importorskip("psycopg2")
pytestmark = pytest.mark.integration

DB_URL = os.environ.get("COMPLIANCE_DB_URL")
SQL_DIR = Path(__file__).parents[2] / "sql" / "init"


@pytest.fixture
def conn_factory() -> Iterator[Callable[[], Any]]:
    if not DB_URL:
        pytest.skip("COMPLIANCE_DB_URL not set")
    schema = f"t_{uuid.uuid4().hex[:8]}"
    admin = psycopg2.connect(DB_URL)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
        for f in ("01_tables.sql", "02_views.sql"):
            cur.execute((SQL_DIR / f).read_text())

    def factory() -> Any:
        return psycopg2.connect(DB_URL, options=f"-c search_path={schema}")

    yield factory
    with admin.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    admin.close()


@pytest.fixture
def gold_paths(tmp_path: Path, fixture: Callable[[str], Any]) -> tuple[Storage, Path, Path]:
    storage = Storage(tmp_path / "data")
    write_bronze(storage, fixture, pick(fixture, 699, 681))
    silver = SilverBuilder(storage).build(DS)
    gold = GoldBuilder(storage, RuleConfig()).build(silver.frames, DS)
    return storage, gold.pr_compliance_path, gold.summary_path


def count(factory: Callable[[], Any], sql: str) -> Any:
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


def test_load_is_idempotent(conn_factory: Callable[[], Any], gold_paths: Any) -> None:
    _, gold, summary = gold_paths
    loader = PostgresLoader(conn_factory)
    first = loader.load(gold, summary, DS)
    assert (first.inserted, first.updated) == (2, 0)
    second = loader.load(gold, summary, DS)
    assert (second.inserted, second.updated) == (0, 2)
    assert count(conn_factory, "SELECT count(*) FROM pr_compliance") == 2
    assert count(conn_factory, "SELECT count(*) FROM compliance_summary") == 2  # __all__ + cli
    reasons = count(
        conn_factory, "SELECT violation_reasons FROM pr_compliance WHERE pr_number = 699"
    )
    assert reasons == []
    assert count(conn_factory, "SELECT count(*) FROM v_compliance_humans_only") == 1


def test_failed_load_rolls_back(
    conn_factory: Callable[[], Any], gold_paths: Any, tmp_path: Path
) -> None:
    _, gold, summary = gold_paths
    df = pd.read_parquet(gold)
    df.loc[1, "pr_title"] = None  # violates NOT NULL on the second row
    bad = tmp_path / "bad.parquet"
    df.to_parquet(bad)
    with pytest.raises(LoadError):
        PostgresLoader(conn_factory).load(bad, summary, DS)
    assert count(conn_factory, "SELECT count(*) FROM pr_compliance") == 0
    assert count(conn_factory, "SELECT count(*) FROM compliance_summary") == 0


def test_data_quality_checks(
    conn_factory: Callable[[], Any], gold_paths: Any, tmp_path: Path
) -> None:
    storage, gold, summary = gold_paths
    PostgresLoader(conn_factory).load(gold, summary, DS)
    settings = Settings(data_dir=storage.data_dir, dq_freshness_hours=48)
    checker = DataQualityChecker(settings, storage, conn_factory)
    window_end = datetime(2026, 10, 6, tzinfo=UTC)  # PR 681 merged 2026-10-05

    results = checker.run(DS, "r1", ["cli"], window_end)
    assert all(r.passed for r in results)

    with pytest.raises(DataQualityError) as info:
        checker.run(DS, "r1", ["cli", "frontend"], window_end)  # frontend never extracted
    assert "completeness" in str(info.value)

    with pytest.raises(DataQualityError) as info:
        checker.run(DS, "r1", ["cli"], datetime(2026, 12, 1, tzinfo=UTC))
    assert "freshness" in str(info.value)

    with pytest.raises(DataQualityError) as info:
        checker.run(DS, "another-run", ["cli"], window_end)  # stale manifest from earlier run
    assert "completeness" in str(info.value)
