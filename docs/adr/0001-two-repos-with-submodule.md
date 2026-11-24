# 1. Two repositories joined by a git submodule

**Status:** accepted

**Context.** Pipeline code (what runs) and runtime (where it runs) change at different
rates and would be owned by different people in a real team. The reviewer still needs
one clone and one command.

**Decision.** `pr-compliance-processing` owns the package, DAG, JSON Schemas and the
output DDL. `pr-compliance-infra` owns Docker, compose, Grafana and pins processing as
a submodule at `processing/`. The DAG and DDL live with the code because they change in
the same commit as the code; infra only decides where it runs.

**Consequences.** Upgrading = bump the submodule and rebuild. Submodules are clumsy:
`make check` fails fast with a clear message when `processing/` is empty (cloned
without `--recursive`). The submodule URL is relative (`../pr-compliance-processing`)
so it resolves to the sibling repo on any host.
