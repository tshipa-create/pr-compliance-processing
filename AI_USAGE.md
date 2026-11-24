# AI usage

> **Note to self before submitting:** this file must be *my* account. The facts below
> are recorded from the build session; the items marked ✍️ need my own words.

## Tools and roles

| Tool | Role |
|---|---|
| Claude Code (Claude Opus 5.5) in the terminal | Implementation from my build guide: package, tests, DAG, compose, CI, docs; ran the stack and the tests |
| ✍️ *(any other tool used for the build guide / research)* | |

Context given to it: the assignment PDF, my own build guide
(`Scytale Data Engineer Assignment — Full Build Guide.md`), [`CLAUDE.md`](CLAUDE.md),
live GitHub API responses, and the official Airflow 3.3.0 `docker-compose.yaml`,
constraints file and image (inspected with `docker run`). No MCP servers.

## Where AI did the heavy lifting vs. where I didn't use it

* Heavy lifting: boilerplate (client, storage, loader, compose, CI), test scaffolding,
  docs drafts.
* ✍️ Decided / written by me: *(e.g. compliance rule semantics, retry policy, the DAG
  shape, what is retryable vs. fatal)*.

## Prompts (verbatim)

1. `lets look into docs folder and do the assignment`
2. Answers to its two clarifying questions: *"Two repos per guide"* and extras *"Mailpit
   email alerts, Grafana dashboard, GitHub Actions CI"* (DuckDB deliberately skipped).
3. ✍️ *A prompt I had to reframe:* …

## What was rejected or corrected

Every item below was caught by a test or by running the real stack, not by reading:

| Problem | How it was caught | Fix |
|---|---|---|
| Guide/model assumed Python 3.12; the `apache/airflow:3.3.0` image ships **3.13** with pandas 2.3.3 / numpy 2.5 | `docker run … python --version` | Constraints switched to `constraints-3.13.txt` |
| `schedule="0 0 * * *"` in Airflow 3 gives `CronTriggerTimetable` (zero-length data interval) → every run would process nothing | Read `create_cron_data_intervals=False` in the image config | `CronDataIntervalTimetable` + DAG test asserting it |
| Manual triggers in Airflow 3 have `logical_date=None` and no data interval → crash | Triggered a run; `data_interval_start None` | `Window.previous_day(run_after)` |
| `pendulum.DateTime - timedelta` returns a **naive** datetime → "can't compare offset-naive and offset-aware" | Real DAG run with `LOOKBACK_DAYS` | `to_utc()` at the boundary + regression test |
| Logging to stderr: Airflow 3 re-tags it `task.stderr` at **ERROR** and double-encodes the JSON; its stdlib bridge drops `extra` fields | Read a real task log | structlog bridge handler (ADR 0008) |
| `dataclasses.asdict()` deep-copied a `threading.Lock` → crash in every task's metrics | Unit test | Explicit field copy |
| `extra={"name": …}` collides with a reserved `LogRecord` attribute | Integration test | Renamed to `check` |
| DB password `compliance` → Airflow's secret masker turned `pr_compliance` into `pr_***` in logs | Read a real task log | Non-dictionary default password |
| "Latest review per reviewer" would let a COMMENTED review cancel an APPROVED one (GitHub does not) | Rule test | Only decisive states count (ADR 0006) |
| `DagBag(include_examples=…)` no longer exists in 3.3 | Ran the DAG test in the image | Removed |
| Permanent errors (404 repo, 401, failed DQ) were still retried by Airflow | Failure-path run | `AirflowFailException` for permanent errors |
| `trigger_rule="all_done"` (from the guide) let `transform` load stale bronze when `list_repos` itself failed | Failure-path run | `all_done_min_one_success` + bronze filtered by `run_id` |
| Upsert-only load left orphan rows after a narrower rerun of the same day (318 in Postgres vs 304 in gold) | The pipeline's own `integrity` DQ check | Run-owned load scope: upsert + delete within (repos × window) |

Hallucinated GitHub fields: none survived — every schema field is backed by a trimmed
real payload in `tests/fixtures/`, and the schema tests validate those payloads.

## How it was verified

* `uv run pytest` — 92 unit + 4 Postgres integration tests; `ruff`, `mypy --strict`.
* DAG integrity test inside `apache/airflow:3.3.0`.
* Full DAG runs in Docker against the live GitHub API (85 repos, 304 PRs), a manual
  spot-check of a non-compliant PR on GitHub, and a deliberate failure run to see the
  fail-fast → healthy repos still loaded → DQ failure → e-mail alert path, and the
  `make demo` manual-trigger path.
