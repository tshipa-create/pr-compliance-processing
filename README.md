# pr-compliance-processing

Pipeline code for the GitHub pull-request compliance pipeline: a typed Python package,
the Airflow 3.3 DAG that wires it, the JSON Schemas that define the raw input, and the
Postgres DDL that defines the output.

**To run it, use the infra repo** (`pr-compliance-infra`, which pins this repo as its
`processing/` submodule): `make up`. Its README is the main documentation: architecture,
configuration, rules, logging, alerting, runbook.

## Module map

```
dags/pr_compliance_dag.py        wiring only (list_repos → extract[repo] → transform → load → data_quality_check)
src/pr_compliance/
├── pipeline.py                  one entry point per task; plain values in, paths/counts out
├── config.py                    pydantic-settings: every env var in one place
├── logging_config.py            JSON logs; bridged into Airflow's structlog inside tasks
├── exceptions.py                PipelineError hierarchy
├── metrics.py                   RunMetrics, tracked_task(), etl_runs recorder
├── storage.py                   medallion paths, atomic writes, partition swaps
├── alerts.py                    on_failure / on_retry / deadline callbacks → e-mail
├── github/
│   ├── client.py                GitHubClient: auth, timeouts, Link pagination, retries, _classify()
│   ├── rate_limiter.py          thread-safe X-RateLimit-* tracking, sleep until reset
│   └── models.py                Window, Manifest, entity names
├── extract/extractor.py         PRExtractor: repos, merged PRs in window, concurrent per-PR fetch
├── schemas/*.schema.json        JSON Schema (Draft 2020-12) per raw entity
├── transform/
│   ├── schema_loader.py         compile schemas, derive Pandas dtypes from them
│   ├── silver.py                validate → quarantine / flatten → typed Parquet
│   ├── rules.py                 compliance rules (pure functions)
│   └── gold.py                  pr_compliance + summary, pandera-validated
├── load/loader.py               COPY → staging → upsert, one transaction
└── quality/checks.py            data-quality checks
sql/init/                        01_tables.sql, 02_views.sql, 03_grafana_user.sql
tests/                           unit/, integration/, test_dag.py, fixtures/ (real payloads)
docs/                            APPROACH.md, adr/
sample_output/                   files from a real run
```

## Develop

Requires [uv](https://docs.astral.sh/uv/). Python 3.13 matches the
`apache/airflow:3.3.0` image; library versions are constrained to Airflow's
constraints file (`[tool.uv] constraint-dependencies`).

```bash
uv sync                                    # venv + dev tools
uv run pytest -m "not integration"         # 92 unit tests, ~20 s (4 integration tests below)
docker run -d --name pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=compliance -p 55432:5432 postgres:16.16
COMPLIANCE_DB_URL=postgresql://postgres:test@localhost:55432/compliance uv run pytest -m integration
uv run ruff check . && uv run ruff format --check . && uv run mypy   # mypy --strict
```

The DAG integrity test needs Airflow; run `make test-dag` in the infra repo (it runs the
tests inside the Airflow image). Pre-commit hooks: `uvx pre-commit install`.

## Documents

* [`docs/APPROACH.md`](docs/APPROACH.md) — the one-page approach
* [`docs/adr/`](docs/adr) — decision records
* [`AI_USAGE.md`](AI_USAGE.md) — how AI tools were used and verified
* [`CLAUDE.md`](CLAUDE.md) — the context file given to the AI tools
* [`sample_output/`](sample_output) — real output files
