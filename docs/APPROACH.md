# Approach

## What it answers

*Which merged pull requests in the `home-assistant` organisation met our code-review
standard (at least one approval) and status-check standard (every check green), and how
does that vary by repository and over time?*

## Shape

```
list_repos ──► extract[repo] ×N (mapped, 3 at a time) ──► transform ──► load ──► data_quality_check
                 │ bronze JSON                              │ silver/gold    │ Postgres     │ fails the run
                 ▼                                          ▼ Parquet        ▼ upsert       ▼ → e-mail alert
```

A daily Airflow 3.3 DAG (00:00 UTC) processes the PRs merged during its data interval,
the previous UTC day. Each task writes its layer to disk and passes only a path or a
count to the next one.

* **Extract** — a small typed GitHub client (timeouts, `Link`-header pagination,
  `X-RateLimit-*` throttling shared safely across threads, tenacity retries honouring
  `Retry-After`). The closed-PR list is read newest-updated first and pagination stops
  as soon as `updated_at` falls before the window, so a daily run reads one or two pages
  per repo instead of the full history (the 10k+ PR repos would otherwise cost hundreds
  of calls). Per-PR resources are fetched concurrently (8 threads). Raw responses are
  written per repo into a bronze partition that is staged and swapped in atomically.
* **Transform** — every raw record is validated against a JSON Schema (Draft 2020-12)
  that also defines the Pandas dtype of every silver column; invalid records go to
  quarantine with the error path. Compliance rules are pure functions over DataFrames;
  gold is validated with pandera before it is written.
* **Load** — `COPY` into a temp staging table, then `INSERT … ON CONFLICT DO UPDATE` in
  one transaction; returns inserted vs updated counts (`xmax = 0`). The summary table is
  replaced per `run_date` in the same transaction.
* **Data quality** — completeness (every repo extracted *by this run*), quarantine rate,
  volume, compliance drift vs a 7-day average, freshness, and Postgres-vs-gold row
  counts. Any failure fails the run, which triggers the alert.

## Design choices and trade-offs

| Choice | Why | Cost accepted |
|---|---|---|
| Thin DAG, typed package | Logic testable without Airflow; DAG parses in ~50 ms | One extra module (`pipeline.py`) |
| Data interval, not a watermark | Deterministic, idempotent reruns and backfills | Must use `CronDataIntervalTimetable` explicitly in Airflow 3 |
| Dynamic task mapping per repo | Isolated retries, parallelism, readable UI | ~85 task instances per run |
| `transform` with `trigger_rule=all_done` | One broken repo does not block 84 healthy ones | DQ task must (and does) fail the run |
| Medallion folders | Rebuild silver/gold from bronze when a rule changes | Local disk only |
| REST, not GraphQL | The brief names the REST endpoints | ~4× more calls (ADR 0002) |
| Pandas, LocalExecutor | Thousands of rows/day | Single node |
| Conservative rules | Compliance should not give benefit of the doubt | Optional checks (e.g. codecov) can fail a PR |

## Results from a real run (2026-11-23)

85 repositories, 304 merged PRs, ~1,300 API calls (26% of the hourly quota), about 8
minutes end to end. Compliance 91.5% overall; the main violation is failed checks (21)
followed by missing approval (9). `android` stands out at 29% (bots and unapproved PRs).

## Verification

* 89 unit tests on trimmed **real** payloads (pagination, rate limits, retries, window
  edges, every rule edge case, schema failures, edge payloads such as deleted users).
* 3 integration tests against Postgres 16 (idempotent upsert, rollback on a bad row,
  data-quality checks).
* DAG integrity test inside the `apache/airflow:3.3.0` image.
* Full DAG runs in Docker against the live API, a spot-check of a non-compliant PR
  against GitHub, and a deliberate failure (non-existent repo) to exercise retries,
  `all_done`, the DQ failure and e-mail alerts.

## With more time

* Overlap each window by a few hours to close the "PR updated mid-pagination" gap.
* ETag / `If-None-Match` caching (304s are free against the rate limit).
* Read required checks from branch protection when an admin token is available.
* GitHub webhooks (`pull_request.closed`) instead of polling at higher scale.
