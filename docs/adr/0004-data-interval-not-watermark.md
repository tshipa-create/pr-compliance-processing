# 4. Airflow data interval instead of a watermark

**Status:** accepted

**Context.** The pipeline must decide which PRs each run processes and be safe to
rerun.

**Decision.** Each run processes PRs merged in its data interval
`[data_interval_start, data_interval_end)` (the previous UTC day), using
`CronDataIntervalTimetable("0 0 * * *")`. Airflow 3's default for a cron string,
`CronTriggerTimetable`, has a zero-length interval and would process nothing, so the
timetable is explicit. Manual triggers (no interval in Airflow 3) process the last full
UTC day before they were created. `LOOKBACK_DAYS` widens the window for demos.

**Consequences.** No state file to corrupt; reruns are deterministic and overwrite the
same `ds` partition and upsert the same rows; history is loaded with backfills. Known
gap: a PR updated *while* we paginate can move to page 1 after we read it; overlapping
windows plus the upsert would close it.
