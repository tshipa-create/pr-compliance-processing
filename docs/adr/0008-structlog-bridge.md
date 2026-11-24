# 8. Package logs through Airflow's structlog inside tasks

**Status:** accepted

**Context.** The package uses plain stdlib logging (so it is testable without Airflow).
Airflow 3 writes task logs as JSON through structlog, but (a) anything a task prints to
stderr is re-tagged `logger=task.stderr, level=error`, and (b) its bridge for stdlib
records drops `extra=` fields. A JSON handler on stderr therefore produced INFO lines
shown as errors, with our JSON double-encoded inside the `event` string.

**Decision.** `configure_logging()` installs a small handler that re-emits our records
through structlog with every field as a key/value when Airflow has configured structlog,
and a stdlib JSON formatter otherwise (tests, CLI).

**Consequences.** One log format everywhere: correct levels, first-class fields
(`repo`, `pr_number`, `remaining`, `sleep_s`...) in the Airflow UI and log files.
