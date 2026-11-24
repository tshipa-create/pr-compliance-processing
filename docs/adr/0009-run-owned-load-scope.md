# 9. Each run owns a slice of the output table

**Status:** accepted

**Context.** A plain upsert is idempotent for identical reruns but never removes rows.
During testing, a rerun of the same day with a narrower scope (fewer repos, shorter
window) left 14 orphan rows; the `integrity` data-quality check caught it (318 rows in
Postgres vs 304 in gold). Truncating by `run_date` would be wrong too: it would wipe the
rows of repos whose extraction failed in this run.

**Decision.** `transform` writes `load_scope.json`: the repos this run extracted and its
merge window. `load`, in one transaction, upserts gold and then deletes rows inside that
scope that are no longer in gold. The `integrity` check counts rows in the same scope.

**Consequences.** Reruns, narrower reruns and backfills converge to exactly what gold
says; repos outside the scope (failed this run) keep their earlier rows. A PR that is
quarantined on a rerun disappears from the table — visible through the quarantine-rate
check.
