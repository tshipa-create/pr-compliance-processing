# PR Compliance Pipeline — Design Doc

Nov 24, 2026 · @Tshipa

> **Status:** written before the build and kept as written. Running the real stack
> changed the decisions below; the code follows this table, and the reasons are in
> [`../AI_USAGE.md`](../AI_USAGE.md) and [`adr/`](adr/).
>
> | This doc says | Built | Why |
> |---|---|---|
> | Python 3.12 (CI) | Python 3.13 | The `apache/airflow:3.3.0` image ships 3.13 |
> | `schedule="0 0 * * *"` | `CronDataIntervalTimetable("0 0 * * *")` | In Airflow 3 a cron string gives each run a zero-length data interval ([ADR 0004](adr/0004-data-interval-not-watermark.md)) |
> | `transform` uses `trigger_rule="all_done"` | `all_done_min_one_success`, and silver reads only this run's bronze (`run_id`) | `all_done` reloaded stale bronze when `list_repos` itself failed |
> | Latest review per reviewer | Latest *decisive* review (`APPROVED` / `CHANGES_REQUESTED`) per reviewer | A later `COMMENTED` review would otherwise cancel an approval ([ADR 0006](adr/0006-compliance-rule-choices.md)) |
> | Load = upsert only | Upsert, then delete rows in this run's scope (repos × window) that are no longer in gold | Upsert alone left orphan rows after a narrower rerun (318 vs 304) ([ADR 0009](adr/0009-run-owned-load-scope.md)) |
> | — | Manual triggers fall back to the previous UTC day | Manual runs in Airflow 3 have no data interval |

## Overview

Build a daily Airflow 3.3 pipeline that pulls merged pull requests from the `home-assistant` GitHub organisation, decides whether each one met code-review and status-check standards, and loads the result into Postgres. Split it across two repos (infra and processing), lay the data out as bronze, silver and gold, and back it with tests, CI, monitoring and clear docs.

&#91;embedded content: pipeline architecture · 5 tasks, 3 data layers\]

Each task writes its layer to disk and passes only the path on; the quality check fails the run, and so alerts, when anything is off.

**Design principles**

- **Thin DAG, fat library.** All logic lives in a typed Python package; the DAG only wires tasks. Everything is unit-testable without Airflow.
- **Pass paths, not data.** Tasks exchange file paths through XCom, never DataFrames.
- **Idempotent by logical date.** A rerun of the same day overwrites the same files and upserts the same rows.
- **Fail loudly, degrade gracefully.** One bad PR or repo is logged and skipped; a systemic failure fails the task and alerts.
- **Simple over clever.** LocalExecutor, Pandas, plain Parquet. Over-engineering is explicitly marked down.

**What reviewers grade** (from the brief): clean production-ready OOP code, data-engineering best practice, API edge cases, robust logging, clear docs, and your ability to explain every line in the follow-up call.

## Two-repo layout

Use two repos: **processing** owns the pipeline code, DAG, schemas and table DDL; **infra** owns the runtime and pins processing as a git submodule at a release tag. The reviewer still clones one repo and runs one command.

**`pr-compliance-processing`**

```
pr-compliance-processing/
├── pyproject.toml              # package, uv groups + constraints, ruff, mypy, pytest (uv.lock committed)
├── dags/
│   └── pr_compliance_dag.py    # thin wiring only
├── src/pr_compliance/
│   ├── config.py               # pydantic-settings, all env config
│   ├── logging_config.py       # unified JSON logger
│   ├── exceptions.py           # error hierarchy
│   ├── metrics.py              # RunMetrics dataclass -> etl_runs
│   ├── storage.py              # bronze/silver/gold paths, atomic writes
│   ├── github/
│   │   ├── client.py           # GitHubClient: session, pagination, retry
│   │   ├── rate_limiter.py     # thread-safe, reads X-RateLimit-*
│   │   └── models.py           # TypedDicts / dataclasses
│   ├── extract/extractor.py    # PRExtractor
│   ├── transform/
│   │   ├── schema_loader.py    # JSON Schema -> validator + dtypes
│   │   ├── silver.py           # validate, flatten, quarantine
│   │   ├── rules.py            # ComplianceRules (pure functions)
│   │   └── gold.py             # compliance table + summary
│   ├── load/loader.py          # PostgresLoader, DuckDB export
│   └── quality/checks.py       # data-quality checks
├── schemas/                    # *.schema.json per raw entity
├── sql/init/                   # 01_tables.sql, 02_views.sql
├── tests/  (unit/, integration/, fixtures/, cassettes/)
├── sample_output/
├── docs/ (APPROACH.md, adr/)
├── AI_USAGE.md
├── CLAUDE.md
└── .github/workflows/ci.yml
```

**`pr-compliance-infra`**

```
pr-compliance-infra/
├── processing/                 # git submodule @ vX.Y.Z
├── docker-compose.yml
├── docker-compose.override.yml # dev: live-mount processing/src
├── Dockerfile                  # FROM apache/airflow:3.3.0
├── grafana/                    # optional: datasource + dashboard JSON
├── .env.example
├── Makefile                    # up, down, demo, logs, trigger, test
├── README.md                   # main entry point for reviewers
└── .github/workflows/ci.yml
```

**Why the DAG and DDL live in processing:** the DAG calls the package's functions and the DDL is the output contract, so both change in the same commit as the code. Infra decides where things run, not what they do.

**Wiring:** the infra Dockerfile runs `pip install ./processing` and copies `processing/dags/`. The compliance Postgres mounts `processing/sql/init` into `/docker-entrypoint-initdb.d`. Upgrading = bump the submodule tag and rebuild.

**Reviewer path:**

```
git clone --recursive <infra-repo>
cp .env.example .env      # add GITHUB_TOKEN
make up                   # UI at http://localhost:8080
make demo                 # small end-to-end run
```

The Makefile should fail fast with a clear message if `processing/` is empty (forgot `--recursive`).

## Docker and Airflow 3.3 environment

Start from the official Airflow 3.3.0 `docker-compose.yaml`, switch it to LocalExecutor, drop Redis and Celery workers, and add a second Postgres for the output.

| Service | Role | Notes |
| --- | --- | --- |
| `airflow-db` | Airflow metadata Postgres | Never holds business data |
| `compliance-db` | Output Postgres | Mounts `processing/sql/init`; exposed on 5433 for reviewers |
| `airflow-init` | DB migrate + admin user | Runs once, then exits |
| `airflow-apiserver` | UI + REST API on 8080 | Replaces the Airflow 2 webserver |
| `airflow-scheduler` | Schedules and runs tasks (LocalExecutor) |  |
| `airflow-dag-processor` | Parses DAG files | Separate process in Airflow 3 |
| `airflow-triggerer` | Deferrable tasks | Keep for completeness |
| `grafana` (optional) | Dashboard on the output DB | Provisioned from files |

Add one more service, **`mailpit`** (`axllent/mailpit`): a local SMTP server with a web inbox on http://localhost:8025. Airflow sends alert emails to it, so reviewers can see alerts with no Slack, Gmail or other outside account.

**Configuration**

- Everything from `.env`: `GITHUB_TOKEN`, `AIRFLOW_UID`, DB credentials, `LOOKBACK_DAYS`, `MAX_PRS_PER_REPO`, `REPO_ALLOWLIST`, `ALERT_EMAIL_TO`.
- Connections as env vars: `AIRFLOW_CONN_COMPLIANCE_DB=postgresql://...`, `AIRFLOW_CONN_GITHUB_API` (token in the password field) so the token is never hard-coded or logged.
- Healthchecks on every service; `depends_on: condition: service_healthy` so Airflow waits for its DB.
- Volumes: `./data` for bronze/silver/gold, `./logs` for task logs.
- Pin every image tag. The Dockerfile installs the processing package into the image's own Airflow environment with uv (uv pip install --no-cache -c \<Airflow 3.3.0 constraints> ./processing), so versions match the image. Check whether the 3.3.0 image already ships uv; otherwise copy the binary from uv's official image.

**Airflow 3 gotchas** (models often get these wrong; note any you catch in AI\_USAGE.md)

- Import from `airflow.sdk` (`dag`, `task`, `Asset`), not `airflow.decorators` or `airflow.models.dag`.
- `schedule=`, not `schedule_interval=`; `catchup` defaults to False in 3.x but set it explicitly.
- Task code cannot touch the metadata DB directly; it goes through the Task Execution API.
- SLAs were removed; use deadline alerts (check the 3.3 docs for the exact API).
- Context variables such as `execution_date` are gone; use `logical_date` and `data_interval_start/end`.
- Verify the Postgres provider and Slack provider versions are compatible with 3.3.0.

## Medallion data layout

Medallion is a naming convention here, not new tooling: three folders, partitioned by the run's logical date (`ds`). No Delta Lake, no Spark, no catalogue.

| Layer | Path | Format | Contents |
| --- | --- | --- | --- |
| Bronze | `data/bronze/ds=YYYY-MM-DD/repo=<name>/` | JSON | API responses exactly as returned: `pull_requests.json`, `reviews.json`, `commits.json`, `statuses.json`, `check_runs.json`, plus `_manifest.json` (counts, timings, API calls) |
| Quarantine | `data/quarantine/ds=YYYY-MM-DD/` | JSON | Records that failed schema validation, with the error |
| Silver | `data/silver/ds=YYYY-MM-DD/<entity>.parquet` | Parquet | Validated, typed, flattened: one file per entity |
| Gold | `data/gold/ds=YYYY-MM-DD/` | Parquet + JSON | `pr_compliance.parquet`, `summary.parquet`, `summary.json` |

**Rules**

- **Atomic writes:** write to `*.tmp`, then `os.replace`. A crash never leaves a half file.
- **Idempotent:** a rerun of the same `ds` deletes and rewrites that partition only.
- **Bronze is immutable input:** never edited, so silver and gold can be rebuilt without calling GitHub again (e.g. when a compliance rule changes).
- **Parquet:** snappy compression, explicit schema (pyarrow), timestamps in UTC.
- **Retention:** document it (e.g. keep 30 days of bronze locally); `data/` is gitignored except `sample_output/`.

## DAG design

One DAG, `pr_compliance`, runs daily at 00:00 and processes the PRs merged during its data interval (the previous day). It keeps the required extract → transform → load shape, with extract fanned out per repo.

**Tasks**

1. `list_repos` — lists non-archived public repos of the org (or `REPO_ALLOWLIST`), returns a list of names.
2. `extract` — dynamically mapped, one task instance per repo (`extract.expand(repo=...)`), capped with `max_active_tis_per_dag=3`. Writes bronze, returns the partition path.
3. `transform` — bronze → silver → gold for the whole `ds`. Uses `trigger_rule="all_done"` and reads the bronze manifests on disk, so healthy repos still flow through when one repo failed.
4. `load` — gold → Postgres (and optional DuckDB). Declares an `Asset` outlet for data-aware scheduling.
5. `data_quality_check` — runs the checks in the monitoring section and fails the run (so it alerts) if any repo failed extraction or a threshold is breached.

**Sketch** (Airflow 3 syntax)

```python
import pendulum
from datetime import timedelta
from airflow.sdk import Asset, dag, task, get_current_context

from pr_compliance.alerts import notify_failure

COMPLIANCE_TABLE = Asset("postgres://compliance-db/compliance/public/pr_compliance")

@dag(
    dag_id="pr_compliance",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
    tags=["compliance", "github"],
)
def pr_compliance() -> None:

    @task
    def list_repos() -> list[str]: ...

    @task(max_active_tis_per_dag=3)
    def extract(repo: str) -> str:
        ctx = get_current_context()
        window = (ctx["data_interval_start"], ctx["data_interval_end"])
        ...

    @task(trigger_rule="all_done")
    def transform(_: list[str]) -> dict[str, str]: ...

    @task(outlets=[COMPLIANCE_TABLE])
    def load(gold: dict[str, str]) -> int: ...

    @task
    def data_quality_check(rows_loaded: int) -> None: ...

    bronze = extract.expand(repo=list_repos())
    data_quality_check(load(transform(bronze)))

pr_compliance()
```

**Why the data interval instead of a watermark:** each run owns a fixed time window, so reruns are deterministic and history is loaded with `airflow dags backfill` (or a `LOOKBACK_DAYS` override for `make demo`). No state file to corrupt.

**Timezone assumption:** “12:00 AM” is read as midnight UTC. Document it; switching to `Africa/Johannesburg` is a one-line change in `start_date`.

**Failure modes to be ready to explain**

- One repo fails → its extract task fails after retries; others load; the DQ task fails the run and alerts.
- Rate limit exhausted mid-run → the client sleeps until reset; if that exceeds a cap, it raises `GitHubRateLimitError` and the task retries later.
- Crash halfway through a repo → bronze writes are atomic; the retry rewrites that repo's partition.
- Load fails → one transaction, nothing partial; rerun load alone.

## Extract

Extract is where most of the risk is: correct endpoints, pagination, rate limits, retries and edge cases, all behind a small typed client.

**Endpoints**

| Purpose | Endpoint | Notes |
| --- | --- | --- |
| Repos | `GET /orgs/home-assistant/repos?type=public&per_page=100` | Skip archived and empty repos |
| Closed PRs | `GET /repos/{o}/{r}/pulls?state=closed&sort=updated&direction=desc&per_page=100` | Merged = `merged_at` not null; no extra merge call needed |
| Reviews | `GET /repos/{o}/{r}/pulls/{n}/reviews?per_page=100` | Paginate; states include `DISMISSED` and `PENDING` too |
| Commits | `GET /repos/{o}/{r}/pulls/{n}/commits?per_page=100` | GitHub caps this list at 250 commits; flag `commits_truncated` |
| Combined status | `GET /repos/{o}/{r}/commits/{head_sha}/status` | Legacy commit statuses only |
| Check runs | `GET /repos/{o}/{r}/commits/{head_sha}/check-runs?per_page=100` | Has name, conclusion, completed\_at; this is what the spec's “check run” fields need |

Use `head.sha` as the ref: checks run on the PR's last commit, not on the squash/merge commit. Fetch both status endpoints and explain why in the README.

**Window and pagination**

- Walk the closed-PR list newest-updated first and stop once `updated_at < data_interval_start`. This is safe because a PR's `updated_at` is always at or after its `merged_at`.
- Keep PRs whose `merged_at` falls in `[data_interval_start, data_interval_end)`.
- Follow the `Link: rel="next"` header (`response.links`), never hand-built page numbers. Don't use the Search API: it stops at 1,000 results.
- Apply `MAX_PRS_PER_REPO` as a safety valve, logged when hit.

**Rate limits**

- Require `GITHUB_TOKEN` (5,000 requests/hour vs 60 unauthenticated). Fail fast with a clear error if it's missing or returns 401.
- After every response read `X-RateLimit-Remaining` and `X-RateLimit-Reset`. Below a floor (e.g. 50), sleep until reset plus a few seconds, logging the wait.
- Secondary limits: a 403 or 429 with `Retry-After` (or remaining = 0) → wait that long, then retry.
- The headers reflect the token's shared quota, so throttling stays correct across parallel mapped tasks.
- Cost per merged PR ≈ 4 calls + extra pages. Put the formula in the README: daily runs fit easily; a long backfill is spread across runs.
- Bonus: send `If-None-Match` with stored ETags; a 304 does not count against the limit.

**Retries**

- `tenacity`: exponential backoff with jitter (1s → 60s, max 5 attempts) on connection errors, timeouts, 5xx, 429 and secondary 403.
- No retry on 401 (fail the task), 404 or 422 (log, skip that PR, count it in the manifest).
- Timeouts on every request (e.g. 10s connect, 30s read).

**Concurrency**

- Within one extract task, a `ThreadPoolExecutor(max_workers=8)` fetches the four per-PR resources across PRs.
- With 3 mapped tasks that's at most \~24 requests in flight, low enough to avoid secondary limits. Make both numbers configurable.
- The rate limiter is thread-safe (a lock around the shared remaining/reset state).

**Edge cases to handle and test**

- Deleted author → `user` is null; map to `ghost`, keep the PR.
- Fork deleted after merge → `head.repo` is null but `head.sha` still works.
- PR with zero reviews, zero statuses or zero check runs → empty lists, not errors.
- Empty repo → 409 on some endpoints; skip the repo with a warning.
- Renamed or transferred repo → 301; let the client follow redirects.
- Bot authors (`user.type == "Bot"`) → keep, but flag.
- Re-run checks → several runs with the same name; resolved in transform.

**Classes**

- `RateLimiter` — tracks remaining/reset, `wait_if_needed()`, `update(headers)`.
- `GitHubClient` — `get(path, params) -> Response`, `paginate(path, params) -> Iterator[dict]`, retries, timeouts, logging.
- `PRExtractor` — `extract_repo(repo, window) -> Path`: lists PRs, fans out per-PR fetches, writes bronze and the manifest.

```python
def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    url: str | None = f"{self.base_url}{path}"
    query = {"per_page": 100, **(params or {})}
    while url:
        resp = self._get(url, params=query)   # retries + rate limiting inside
        payload = resp.json()
        items = payload.get("check_runs", payload) if isinstance(payload, dict) else payload
        yield from items
        url = resp.links.get("next", {}).get("url")
        query = None                           # next URL already carries the query
```

## Transform

Transform runs in two steps: bronze → silver (is the data clean?) and silver → gold (is the PR compliant?). Keeping them apart means a rule change only reruns silver → gold.

**Step 1: bronze → silver**

- One JSON Schema (Draft 2020-12) per entity in `schemas/`: `pull_request`, `review`, `commit`, `combined_status`, `check_run`. Declare only the fields you use, with types, `required`, enums (e.g. review `state`) and `format: date-time`; allow additional properties so new GitHub fields don't break the run.
- Validate every record with `jsonschema.Draft202012Validator`. Invalid records go to quarantine with the error path and message; the run continues.
- `schema_loader.py` maps schema types to Pandas dtypes (`integer` → `Int64`, `string` → `string`, `date-time` → `datetime64[ns, UTC]`, `boolean` → `boolean`), so the input is “predefined” as the brief asks.
- Flatten nested fields (`user.login` → `author_login`, `base.ref` → `base_ref`) and write one Parquet per entity.

**Step 2: silver → gold — compliance rules**

Each rule is a pure function with its own unit tests. Every choice below is an assumption to list in the README.

| Rule | Logic | Why |
| --- | --- | --- |
| `code_review_passed` | Latest review per reviewer; ignore the PR author, `DISMISSED` and `PENDING`; pass if ≥ 1 reviewer's latest state is `APPROVED` | A later “changes requested” cancels an earlier approval; self-approval isn't review |
| `status_checks_passed` | Dedupe check runs by name (latest `completed_at`); pass if every run is `success`, `neutral` or `skipped` and every legacy status is `success` | Re-runs replace old results |
| No checks at all | `status_checks_passed = False`, reason `no_checks` (configurable) | Conservative for compliance |
| Required checks | Treat all checks as required | Branch-protection API needs admin rights a public token lacks |
| Combined status `pending` with `total_count = 0` | Ignore | Means “no legacy statuses”, not a pending check |
| `is_compliant` | `code_review_passed and status_checks_passed` | As specified |

Failing conclusions: `failure`, `cancelled`, `timed_out`, `action_required`, `stale`, `startup_failure`; a run still `in_progress` at merge time also fails.

**Gold output: `pr_compliance`**

| Column | Type | Source |
| --- | --- | --- |
| `pr_number` | int | `number` |
| `pr_title` | string | `title` |
| `author` | string | `user.login` |
| `repository` | string | repo name |
| `merged_at` | timestamp UTC | `merged_at` |
| `code_review_passed` | bool | rule |
| `status_checks_passed` | bool | rule |
| `is_compliant` | bool | rule |
| `approved_review_count` | int | extra |
| `commit_count` | int | extra |
| `is_bot_author` | bool | extra, `user.type == "Bot"` |
| `violation_reasons` | string\[\] | extra: `no_approval`, `self_approved_only`, `failed_checks`, `no_checks` |
| `organization`, `author_id`, `base_ref`, `head_ref` | various | metadata from the brief |
| `run_date` | date | the `ds` |

Validate the gold DataFrame with a `pandera` schema before writing (types, no nulls in keys, unique `(repository, pr_number)`).

**Summary statistics** (`summary.parquet` + `summary.json`)

- Total PRs, compliant PRs, compliance rate.
- Per repository: PRs, compliance rate, violations by reason.
- The same with bot PRs excluded — an easy, genuine insight.

## Load

Load copies gold Parquet into Postgres through a staging table and an upsert, in one transaction, so it is fast, atomic and safe to rerun.

**Schema** (`sql/init/01_tables.sql`, run automatically when the `compliance-db` container first starts)

```sql
CREATE TABLE IF NOT EXISTS pr_compliance (
    organization          TEXT        NOT NULL,
    repository            TEXT        NOT NULL,
    pr_number             INTEGER     NOT NULL,
    pr_title              TEXT        NOT NULL,
    author                TEXT        NOT NULL,
    author_id             BIGINT,
    is_bot_author         BOOLEAN     NOT NULL DEFAULT FALSE,
    base_ref              TEXT,
    head_ref              TEXT,
    merged_at             TIMESTAMPTZ NOT NULL,
    approved_review_count INTEGER     NOT NULL,
    commit_count          INTEGER     NOT NULL,
    code_review_passed    BOOLEAN     NOT NULL,
    status_checks_passed  BOOLEAN     NOT NULL,
    is_compliant          BOOLEAN     NOT NULL,
    violation_reasons     TEXT[]      NOT NULL DEFAULT '{}',
    run_date              DATE        NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repository, pr_number)
);
CREATE INDEX IF NOT EXISTS ix_prc_merged_at ON pr_compliance (merged_at);
CREATE INDEX IF NOT EXISTS ix_prc_noncompliant ON pr_compliance (repository) WHERE NOT is_compliant;

CREATE TABLE IF NOT EXISTS compliance_summary (
    run_date DATE, repository TEXT, total_prs INT, compliant_prs INT,
    compliance_rate NUMERIC(5,4), violations JSONB,
    PRIMARY KEY (run_date, repository)   -- repository '__all__' for org totals
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id TEXT, task_id TEXT, map_index INT, run_date DATE, status TEXT,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, metrics JSONB, error TEXT,
    PRIMARY KEY (run_id, task_id, map_index)
);
```

**Mechanism**

1. Read gold Parquet; open one transaction via `PostgresHook("compliance_db")` (psycopg underneath).
2. `CREATE TEMP TABLE stg (LIKE pr_compliance) ON COMMIT DROP`.
3. `COPY` the rows into `stg` (much faster than row inserts).
4. `INSERT INTO pr_compliance SELECT ... FROM stg ON CONFLICT (repository, pr_number) DO UPDATE SET ...`.
5. Same for `compliance_summary`; write an `etl_runs` row; commit. Any error → rollback, raise `LoadError`.
6. Return inserted vs updated counts (use `RETURNING (xmax = 0)` to tell them apart).

**Views** (`sql/init/02_views.sql`) so the output answers questions on arrival:

- `v_compliance_by_repo_week` — weekly compliance rate per repo.
- `v_violations` — non-compliant PRs with reasons, newest first.
- `v_compliance_humans_only` — same metrics excluding bot authors.

**Optional DuckDB** — a few lines: `duckdb.sql("SELECT * FROM 'data/gold/*/pr_compliance.parquet'")`, or write `data/compliance.duckdb`. Ticks the brief's optional box and makes local exploration trivial.

## Logging, monitoring and alerting

Monitor four layers — did it run, how did it behave, is the data right, is the infra healthy — with processing emitting the signals and infra collecting and alerting.

**Unified logging**

- One `logging_config.py` sets a JSON formatter for the `pr_compliance` logger; every module uses `logging.getLogger(__name__)`. Output lands in Airflow's task logs.
- Standard fields: `timestamp`, `level`, `logger`, `event`, `run_id`, `task_id`, `map_index`, `repo`, `pr_number`, `duration_ms`, plus event-specific extras.
- Log events, not prose: `rate_limit_wait`, `pr_skipped`, `record_quarantined`, `load_committed`.
- Never log the token or full payloads. INFO for milestones, WARNING for skips and waits, ERROR with `exc_info` before re-raising.

Example line:

```
{"timestamp":"2026-11-25T00:03:12Z","level":"WARNING","event":"rate_limit_wait","run_id":"scheduled__2026-11-25T00:00:00+00:00","task_id":"extract","map_index":0,"repo":"core","remaining":42,"sleep_s":318}
```

**Unified errors** (`exceptions.py`)

- `PipelineError` base → `GitHubAPIError`, `GitHubAuthError`, `GitHubRateLimitError`, `SchemaValidationError`, `LoadError`, `DataQualityError`.
- Retryable vs fatal is decided in one place (the client), not scattered `try/except` blocks.

**1. Did it run? (Airflow)**

- `on_failure_callback` with Airflow's SMTP notifier emails the local Mailpit inbox once, after retries are exhausted.
- `on_retry_callback` logs a warning only.
- A deadline alert if the run hasn't finished by e.g. 03:00 (check the 3.3 API).
- A dead scheduler fails nothing, so the freshness check below is the safety net.

**Alert design: self-contained, no outside accounts**

- One `notify_failure(context)` function does three things: logs a structured `pipeline_alert` line, writes the alert to `etl_runs`, and sends an email through the `smtp_alerts` connection (pointing at `mailpit:1025`).
- Email content: DAG, task, repo (map index), run date, error class and message, link to the task log.
- If sending fails, log `alert_delivery_failed` and never raise: a broken alert must not hide the original error.
- `make test-alert` sends a test alert so reviewers can open the Mailpit inbox and see one immediately.
- Slack would be the first choice in a real deployment: the team sees alerts where it already works, threads them and acts on them. It isn't used here only because the assignment has to run self-contained, and reviewers have no access to my Slack workspace or webhook. Say this in the README and in a short ADR ("Mailpit instead of Slack for alerts"). Since \`notify\_failure\` is the only place alerts are sent, switching to Slack means adding a Slack notifier call and a connection, with no other code changes.
- Check the SMTP notifier's import path against the provider version that works with Airflow 3.3, and note any wrong path an AI suggested in AI\_USAGE.md.

**2. How did it behave? (pipeline metrics)**

Each task fills a `RunMetrics` dataclass, logs it and writes it to `etl_runs`: API calls, retries, minimum `X-RateLimit-Remaining`, seconds slept, PRs fetched and skipped per repo, records valid and quarantined, rows inserted and updated, duration, status, error class.

**3. Is the data right? (`data_quality_check` task)**

| Check | Fails when | Catches |
| --- | --- | --- |
| Extraction completeness | Any repo's extract failed | Partial runs |
| Freshness | Newest `merged_at` older than 48 h | Missed schedules, stuck pipeline |
| Volume | 0 PRs, or < 50% of 7-day average | API or filter bugs |
| Quarantine rate | > 5% of records | GitHub field changes |
| Compliance drift | Rate moves > 20 points vs 7-day average | Rule bugs |
| Integrity | Nulls in keys or duplicate PKs | Load bugs |

Thresholds live in config and the README.

**4. Is the infra healthy?**

- Docker healthchecks; `pg_isready` for both databases.
- The API server's health endpoint (`/api/v2/monitor/health`) for scheduler, triggerer and DAG processor heartbeats.

**Dashboard (Grafana, optional but recommended)**

One Grafana dashboard, built entirely from files in the infra repo, shows both pipeline health and the compliance results at http://localhost:3000, with no login for reviewers. It reads the compliance Postgres directly, so it needs no extra metrics stack.

*Setup files*

```
pr-compliance-infra/grafana/
├── provisioning/
│   ├── datasources/compliance.yml   # Postgres datasource, read-only user
│   └── dashboards/dashboards.yml    # tells Grafana to load the folder below
└── dashboards/pr_compliance.json    # the dashboard, exported from the UI
```

- Mount `provisioning/` at `/etc/grafana/provisioning` and the dashboards folder at the path `dashboards.yml` points to.
- Anonymous read-only access: `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`.
- Create a `grafana_ro` Postgres user with `SELECT` only (in `sql/init/03_grafana_user.sql`); never give the dashboard write access.
- Build the panels in the UI once, export the JSON, commit it. Pin the Grafana image version.

*Panels*

| Row | Panel | Type | Source |
| --- | --- | --- | --- |
| Compliance | Compliance rate, latest run | Stat | `compliance_summary`, `repository = '__all__'` |
| Compliance | Compliance rate over time, all vs humans only | Time series | `compliance_summary` + `v_compliance_humans_only` |
| Compliance | Compliance rate by repository, last 30 days | Bar chart | `pr_compliance` |
| Compliance | Violations by reason | Bar chart | `pr_compliance.violation_reasons` |
| Compliance | Latest non-compliant PRs | Table (PR links) | `v_violations` |
| Pipeline health | Data freshness (hours since newest `merged_at`) | Stat, red over 48 h | `pr_compliance` |
| Pipeline health | Last 14 runs: status and duration per task | Table | `etl_runs` |
| Pipeline health | API calls and minimum rate-limit remaining per run | Time series | `etl_runs.metrics` |
| Pipeline health | Quarantined records per run | Bar chart | `etl_runs.metrics` |

*Example queries*

```sql
-- Compliance rate over time (org level)
SELECT run_date::timestamp AS time, compliance_rate * 100 AS "compliance %"
FROM compliance_summary
WHERE repository = '__all__' AND $__timeFilter(run_date::timestamp)
ORDER BY 1;

-- Compliance by repository, last 30 days
SELECT repository,
       round(100.0 * avg(is_compliant::int), 1) AS "compliance %",
       count(*) AS prs
FROM pr_compliance
WHERE merged_at >= now() - interval '30 days'
GROUP BY repository
ORDER BY "compliance %";

-- Violations by reason
SELECT reason, count(*) AS prs
FROM pr_compliance, unnest(violation_reasons) AS reason
WHERE merged_at >= now() - interval '30 days'
GROUP BY reason
ORDER BY prs DESC;

-- Rate-limit headroom per extract run
SELECT started_at AS time,
       (metrics->>'api_calls')::int AS api_calls,
       (metrics->>'min_rate_limit_remaining')::int AS min_remaining
FROM etl_runs
WHERE task_id = 'extract' AND $__timeFilter(started_at)
ORDER BY 1;
```

*For the submission*

- Put a screenshot of the dashboard after a real run in the README, so reviewers see it before running anything.
- In the demo: open the dashboard right after the Airflow run, then point at freshness and rate-limit headroom as proof the monitoring works.
- Metric names in `etl_runs.metrics` (`api_calls`, `min_rate_limit_remaining`, `records_quarantined`) are a contract with the dashboard: keep them in one `RunMetrics` dataclass and test that they're written.
- Production next step (README only, don't build it): Prometheus scraping Airflow's StatsD/OpenTelemetry metrics, with Grafana alert rules.

**Runbook** (README) — one line per alert: what it means and what to do. For example: *rate limit alert → check token validity, lower `EXTRACT_MAX_WORKERS`, clear the failed task to rerun.*

## Testing strategy

Test in layers: fast unit tests on every push, integration tests against real Postgres in CI, and one real end-to-end DAG run before submitting. Aim for about 80% coverage of `src/`, highest on `rules.py` and `client.py`.

| Layer | Tools | What it covers | Runs in |
| --- | --- | --- | --- |
| Unit | `pytest`, `responses` (mock HTTP), `freezegun` | Client, rules, schema loader, storage | Processing CI, every push |
| Recorded API | `vcrpy` cassettes of real GitHub responses (token scrubbed) | Real payload shapes, pagination | Processing CI |
| DAG integrity | `DagBag` import test | DAG parses, no import errors, 5 tasks, correct schedule and dependencies | Processing CI |
| Integration | `pytest` + Postgres service container (or `testcontainers`) | DDL applies, COPY + upsert, idempotent rerun | Processing CI |
| Smoke / E2E | `docker compose up` + `airflow dags test` | Whole stack, one small repo | Infra CI + manually before submission |

**Must-have unit tests**

- **Pagination:** three pages via `Link` headers → all items yielded, stops on the last page; empty first page.
- **Rate limit:** remaining below the floor → sleeps until reset (mock `time.sleep` and the clock); 403 with `Retry-After` → waits and retries; 429 → retries.
- **Retries:** 502 twice then 200 → succeeds; 404 → no retry, PR skipped; 401 → `GitHubAuthError` raised immediately.
- **Window:** PRs outside `[start, end)` excluded; pagination stops once `updated_at < start`.
- **Review rules:** approved → pass; approved then changes requested by the same reviewer → fail; only the author approved → fail; dismissed approval → fail; two reviewers, one approves → pass.
- **Check rules:** all success → pass; one failure → fail; failed then re-run success with the same name → pass; skipped/neutral → pass; no checks → fail with `no_checks`; pending status with `total_count = 0` → ignored.
- **Schema:** valid record passes; missing required field → quarantined with the reason; extra fields allowed.
- **Edge payloads:** null `user`, null `head.repo`, 250-commit cap flag.

**Integration tests**

- Load the sample gold file twice → row count unchanged, `loaded_at` updated (idempotency).
- Load fails halfway (inject a bad row) → table unchanged (atomicity).

**Practices**

- Fixtures in `tests/fixtures/` are trimmed real payloads, which also back up your AI\_USAGE claim that you checked real fields.
- Rules are pure functions over DataFrames, so tests need no mocks.
- Use `pytest -m "not integration"` for the fast loop; markers for slow tests.
- `make test` runs the same commands CI runs.

## CI/CD with GitHub Actions

Yes, add GitHub Actions to both repos, but keep it to CI plus a tagged release. A green badge on each README is cheap proof the code works; full CD to a cloud is out of scope.

| Repo | Trigger | Jobs |
| --- | --- | --- |
| Processing | push, PR | `lint` (ruff check + format), `typecheck` (mypy --strict), `test` (unit + DAG import, coverage report), `integration` (Postgres service container), sonar (SonarQube Cloud quality gate) |
| Processing | tag `v*` | build the wheel (optionally an image to GHCR), create a GitHub release |
| Infra | push, PR | `validate` (`docker compose config`, hadolint, yamllint), `smoke` (compose up → wait healthy → `airflow dags test` on one small repo → assert rows in Postgres) |
| Both | weekly | Dependabot for pip, GitHub Actions and Docker image updates |

**Processing CI** (`.github/workflows/ci.yml`)

```yaml
name: ci
on: [push, pull_request]
jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }            # Sonar needs full history
      - uses: astral-sh/setup-uv@v6         # pin to the latest release
        with: { enable-cache: true, python-version: "3.12" }
      - run: uv sync --frozen
      - run: uv run ruff check . && uv run ruff format --check .
      - run: uv run mypy src
      - run: uv run pytest -m "not integration" --cov=pr_compliance --cov-report=xml
      - uses: SonarSource/sonarqube-scan-action@v5   # pin to the latest release
        env: { SONAR_TOKEN: "${{ secrets.SONAR_TOKEN }}" }
        with:
          args: >
            -Dsonar.organization=<your-org>
            -Dsonar.projectKey=<your-project-key>
            -Dsonar.python.coverage.reportPaths=coverage.xml
  integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env: { POSTGRES_PASSWORD: test, POSTGRES_DB: compliance }
        ports: ["5432:5432"]
        options: >-
          --health-cmd pg_isready --health-interval 5s --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
        with: { enable-cache: true, python-version: "3.12" }
      - run: uv sync --frozen
      - run: uv run pytest -m integration
        env: { COMPLIANCE_DB_URL: postgresql://postgres:test@localhost:5432/compliance }
```

Match the Python version to the one in the `apache/airflow:3.3.0` image. Airflow and its providers sit in the `dev` dependency group only, constrained by Airflow's constraints file, so CI resolves the same versions as the container.

**Infra smoke test**

- Check out with `submodules: recursive`.
- Pass the workflow's built-in `secrets.GITHUB_TOKEN` as `GITHUB_TOKEN`: it can read public repos, so no personal token is stored.
- Set `REPO_ALLOWLIST` to one small repo and `LOOKBACK_DAYS=3` to keep the run to a couple of minutes.
- On failure, upload the Airflow logs as a workflow artifact for debugging.

**Hygiene**

- `pre-commit` runs ruff, mypy and end-of-file fixers locally, matching CI.
- Protect `main`: CI must pass before merging (and practise what the pipeline checks: approved review + green checks).

**Dependency management with uv**

- uv instead of pip: much faster installs and a committed `uv.lock`; CI and Docker use `--frozen` so nothing resolves differently.
- Keep `apache-airflow==3.3.0` and the Postgres and SMTP providers out of runtime dependencies (the image provides them); put them in the `dev` group for tests, mypy and the DAG import test.
- Apply Airflow's constraints file under `[tool.uv] constraint-dependencies` (or `-c` at install) so versions match the official image. Record this in an ADR.
- Local commands: `uv sync`, `uv run pytest`, `uv run mypy src`, wrapped by the Makefile.

**Code quality with SonarQube Cloud**

- The free plan covers this: unlimited public repos, private repos up to 50k lines, pull request analysis and the default “Sonar way” quality gate.
- Processing repo only; runs after the tests and reads `coverage.xml` (Sonar doesn't produce coverage itself). Use `SonarSource/sonarqube-scan-action`; the older SonarCloud action is deprecated.
- `SONAR_TOKEN` as a repo secret; quality-gate badge in the README. If the repos are private, add a screenshot, since reviewers can't open the dashboard.
- Present it as the quality gate and trend view; ruff and mypy still do the fast checks. Fix or justify every issue it raises.
- Add `pip-audit` (vulnerable dependencies) and Trivy (Docker image scan, infra repo) as one step each.
- Add it last, after everything else works.
