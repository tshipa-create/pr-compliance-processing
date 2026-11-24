-- Output contract of the pipeline. Mounted into the compliance-db container at
-- /docker-entrypoint-initdb.d and run automatically on its first start.

CREATE TABLE IF NOT EXISTS pr_compliance (
    organization          TEXT        NOT NULL,
    repository            TEXT        NOT NULL,
    pr_number             INTEGER     NOT NULL CHECK (pr_number > 0),
    pr_title              TEXT        NOT NULL,
    author                TEXT        NOT NULL,          -- 'ghost' for deleted accounts
    author_id             BIGINT,
    is_bot_author         BOOLEAN     NOT NULL DEFAULT FALSE,
    base_ref              TEXT,
    head_ref              TEXT,
    merged_at             TIMESTAMPTZ NOT NULL,
    approved_review_count INTEGER     NOT NULL CHECK (approved_review_count >= 0),
    commit_count          INTEGER     NOT NULL CHECK (commit_count >= 0),
    commits_truncated     BOOLEAN     NOT NULL DEFAULT FALSE,  -- GitHub caps the list at 250
    code_review_passed    BOOLEAN     NOT NULL,
    status_checks_passed  BOOLEAN     NOT NULL,
    is_compliant          BOOLEAN     NOT NULL,
    violation_reasons     TEXT[]      NOT NULL DEFAULT '{}',
    run_date              DATE        NOT NULL,          -- the Airflow ds that produced the row
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repository, pr_number),
    CONSTRAINT compliant_means_both CHECK (is_compliant = (code_review_passed AND status_checks_passed))
);
CREATE INDEX IF NOT EXISTS ix_prc_merged_at    ON pr_compliance (merged_at);
CREATE INDEX IF NOT EXISTS ix_prc_run_date     ON pr_compliance (run_date);
CREATE INDEX IF NOT EXISTS ix_prc_noncompliant ON pr_compliance (repository) WHERE NOT is_compliant;

COMMENT ON TABLE pr_compliance IS 'One row per merged PR with its code-review and status-check compliance';

CREATE TABLE IF NOT EXISTS compliance_summary (
    run_date              DATE         NOT NULL,
    repository            TEXT         NOT NULL,          -- '__all__' = organisation totals
    total_prs             INTEGER      NOT NULL,
    compliant_prs         INTEGER      NOT NULL,
    compliance_rate       NUMERIC(5,4),
    human_prs             INTEGER      NOT NULL,
    human_compliant_prs   INTEGER      NOT NULL,
    human_compliance_rate NUMERIC(5,4),
    violations            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_date, repository)
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id      TEXT        NOT NULL,
    task_id     TEXT        NOT NULL,
    map_index   INTEGER     NOT NULL DEFAULT -1,
    run_date    DATE        NOT NULL,
    status      TEXT        NOT NULL CHECK (status IN ('success', 'failed')),
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    metrics     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error       TEXT,
    PRIMARY KEY (run_id, task_id, map_index)
);
CREATE INDEX IF NOT EXISTS ix_etl_runs_started ON etl_runs (started_at);
