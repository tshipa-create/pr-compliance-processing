# 2. GitHub REST API rather than GraphQL

**Status:** accepted

**Context.** For each merged PR we need reviews, commits, the combined status and
check runs. GraphQL could fetch all of that for many PRs in one query, roughly 4x
fewer calls. The brief names the REST endpoints explicitly.

**Decision.** Use REST, as specified: 4 calls per merged PR plus extra pages.

**Consequences.** A full daily run of ~85 repos / ~300 PRs costs ~1,300 calls (26% of
the 5,000/h quota). A long backfill is rate-limit bound and the limiter sleeps until
reset. GraphQL is the first lever if volume grows (see README, "At 100x scale").
