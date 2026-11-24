# 5. Medallion layout as plain folders, no table format

**Status:** accepted

**Context.** We need raw data kept for reprocessing, a validated typed layer, and the
business output, at a scale of thousands of rows per day.

**Decision.** Three folders partitioned by `ds`: bronze (raw JSON exactly as returned),
silver (schema-validated, typed Parquet, one file per entity; invalid records go to
`quarantine/`), gold (`pr_compliance.parquet`, `summary.parquet`, `summary.json`).
Writes are atomic (`*.tmp` + `os.replace`; a repo's bronze partition is staged in a temp
directory and swapped in whole).

**Consequences.** A rule change reruns silver -> gold from bronze without calling
GitHub. No Delta/Iceberg, no catalogue: at 100x we would move to object storage and a
table format.
