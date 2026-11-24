# 7. Mailpit (local SMTP) instead of Slack for alerts

**Status:** accepted

**Context.** Alerts must be demonstrable by a reviewer with no access to my Slack
workspace or any external account.

**Decision.** Failure callbacks and the deadline alert send e-mail through the
`smtp_alerts` Airflow connection to a Mailpit container (inbox on :8025).

**Consequences.** Fully self-contained. In production Slack would be the first choice
(the team sees, threads and acts on alerts where it already works). All alerts go
through `pr_compliance.alerts`, so switching means replacing `send_email` with a Slack
notifier and adding a connection — no other change.
