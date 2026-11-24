# Changelog

## v1.0.0 — 2026-11-24

First release.

- GitHub client with Link-header pagination, `X-RateLimit-*` throttling, tenacity
  retries (`Retry-After` aware) and a single retry-vs-fail decision point.
- Extractor: merged PRs in the run's data interval with early pagination stop,
  concurrent per-PR fetches, atomic per-repo bronze partitions with manifests.
- JSON Schema-validated silver layer with quarantine; schema-derived dtypes.
- Compliance rules (latest decisive review per reviewer, latest check run per name,
  legacy statuses) and pandera-validated gold output with summaries.
- Postgres DDL and views; COPY + upsert loader in one transaction.
- Data-quality checks; e-mail alerts; deadline alert; `etl_runs` metrics.
- Airflow 3.3 DAG with dynamic task mapping and explicit data-interval timetable.
