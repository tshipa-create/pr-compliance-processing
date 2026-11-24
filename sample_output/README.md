# Sample output

Real files from the scheduled run `scheduled__2026-11-24T00:00:00+00:00`, which
processed PRs merged on **2026-11-23 (UTC)** across all 85 public, non-archived
repositories of `home-assistant`: 304 merged PRs, 91.5% compliant.

| Path | What it is |
|---|---|
| `bronze/ds=2026-11-23/repo=builder/`, `repo=installer/` | Raw API responses for two repos, exactly as returned, plus `_manifest.json` (window, run_id, counts, API calls, timing) |
| `silver/ds=2026-11-23/*.parquet` | Schema-validated, typed, flattened entities for the whole org; `_stats.json` = valid/quarantined counts (0 quarantined) |
| `gold/ds=2026-11-23/pr_compliance.parquet` | The output table: one row per merged PR |
| `gold/ds=2026-11-23/summary.parquet`, `summary.json` | Org totals (`__all__`) and per-repo stats, violations by reason, humans-only rate |
| `gold/ds=2026-11-23/load_scope.json` | Repos × merge window this run owns in Postgres |
| `gold/pr_compliance_preview.csv` | The gold table as CSV for a quick look |
| `postgres_queries.txt` | `psql` output of the summary, views and `etl_runs` after the load |
| `alert_email_example.eml` | A real alert e-mail from the failure test (repo that does not exist) |
| `logs/*.jsonl` | Excerpts of real task logs: an extract, the data-quality checks, a failed extract |

Read the Parquet files with `pandas.read_parquet(...)` or
`duckdb -c "SELECT * FROM 'gold/ds=2026-11-23/pr_compliance.parquet' LIMIT 10"`.
