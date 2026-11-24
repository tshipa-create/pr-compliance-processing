# 3. LocalExecutor rather than Celery

**Status:** accepted

**Context.** One machine, five tasks, one mapped task with ~85 instances capped at 3
concurrent. The official compose file ships CeleryExecutor with Redis and workers.

**Decision.** LocalExecutor; Redis and Celery workers removed.

**Consequences.** Fewer moving parts and less memory. No horizontal scaling: if the
pipeline ever needed several worker machines, switch to Celery or Kubernetes executor
with no DAG change.
