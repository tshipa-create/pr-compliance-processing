# 6. Compliance rule interpretation

**Status:** accepted

**Context.** "At least one approved review" and "all required checks successful" need
precise definitions to be testable.

**Decision.**
- Only each reviewer's latest *decisive* review counts (APPROVED, CHANGES_REQUESTED,
  DISMISSED). COMMENTED and PENDING never change approval state — that is GitHub's own
  behaviour, and a plain "latest review per reviewer" rule would wrongly cancel an
  approval followed by a comment.
- The author's own reviews and reviews submitted after the merge are ignored.
- Check runs are de-duplicated per (app, name), keeping the latest run. Passing
  conclusions: success, neutral, skipped. A run still in progress at merge fails.
- Legacy commit statuses (latest per context) must be `success`. A combined status of
  `pending` with `total_count = 0` means "no legacy CI" and is ignored.
- All checks are treated as required: branch-protection settings need admin rights
  that a read-only token lacks.
- No checks at all fails (`no_checks`), configurable with `NO_CHECKS_PASSES`.
- `MIN_APPROVALS` (default 1) and `EXCLUDE_BOTS` are one-line config changes.

**Consequences.** Conservative: optional checks such as `codecov/project` can fail a PR
that GitHub allowed to merge. That is visible in `violation_reasons` and is the main
known limitation.
