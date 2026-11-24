# CLAUDE.md — context for AI coding tools

Pipeline code for the PR compliance assignment. Runtime (Docker, compose, Grafana)
lives in the sibling repo `pr-compliance-infra`, which pins this repo as a submodule.

## Layout
- `src/pr_compliance/` — all logic. Typed, no Airflow imports (except lazy ones in
  `alerts.send_email`). Entry points per task in `pipeline.py`.
- `dags/pr_compliance_dag.py` — wiring only. Heavy imports go *inside* task functions.
- `src/pr_compliance/schemas/` — JSON Schema (Draft 2020-12) per raw GitHub entity.
- `sql/init/` — output DDL; mounted into the compliance Postgres on first start.
- `tests/fixtures/` — trimmed **real** GitHub payloads (home-assistant/cli #699, #681).

## Hard rules
- **Airflow 3.3 only.** Import from `airflow.sdk` (`dag`, `task`, `Asset`,
  `get_current_context`, `Connection`, `CronDataIntervalTimetable`). Never
  `airflow.decorators`, `airflow.operators.python`, `schedule_interval`,
  `execution_date`, or SLAs.
- A cron *string* schedule gives `CronTriggerTimetable` (zero-length data interval) in
  Airflow 3 — use `CronDataIntervalTimetable` explicitly.
- Manual triggers have `logical_date=None` and no data interval — handle it
  (`Window.previous_day`).
- Airflow hands tasks `pendulum.DateTime`; normalise with `github.models.to_utc`
  before doing arithmetic (pendulum - timedelta can come back naive).
- **Never invent GitHub API fields.** Check the fixture or the REST docs first. Review
  states: APPROVED, CHANGES_REQUESTED, COMMENTED, DISMISSED, PENDING. Check runs come
  from `/commits/{sha}/check-runs` (`check_runs[]`), legacy statuses from
  `/commits/{sha}/status`.
- Python 3.13 (the image's version). Library versions follow Airflow's
  constraints-3.13 file (`[tool.uv] constraint-dependencies`).
- Logging: `logging.getLogger(__name__)`, message = short snake_case event name,
  details in `extra=`. Never log the token or full payloads. Do not use reserved
  LogRecord keys (`name`, `msg`, `args`, ...) in `extra`.
- Errors: raise from `exceptions.py`. Retry-vs-fail is decided only in
  `GitHubClient._classify`.
- Tasks exchange paths/counts via XCom, never DataFrames.
- Keep it simple: Pandas, LocalExecutor, plain Parquet. No Spark/Celery/K8s/Delta.

## Commands
```bash
uv sync                                  # Python 3.13 venv with dev deps
uv run pytest -m "not integration"       # fast unit tests
COMPLIANCE_DB_URL=postgresql://u:p@localhost:5432/db uv run pytest -m integration
uv run ruff check . && uv run ruff format --check . && uv run mypy
```
DAG integrity test needs Airflow: run `make test-dag` in the infra repo.
