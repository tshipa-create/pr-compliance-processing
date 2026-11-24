-- Views so the output answers common questions on arrival.

CREATE OR REPLACE VIEW v_compliance_by_repo_week AS
SELECT repository,
       date_trunc('week', merged_at)::date                     AS week,
       count(*)                                                AS prs,
       count(*) FILTER (WHERE is_compliant)                    AS compliant_prs,
       round(avg(is_compliant::int)::numeric, 4)               AS compliance_rate
FROM pr_compliance
GROUP BY repository, date_trunc('week', merged_at);

CREATE OR REPLACE VIEW v_violations AS
SELECT organization, repository, pr_number, pr_title, author, is_bot_author, merged_at,
       approved_review_count, code_review_passed, status_checks_passed, violation_reasons,
       format('https://github.com/%s/%s/pull/%s', organization, repository, pr_number) AS pr_url
FROM pr_compliance
WHERE NOT is_compliant
ORDER BY merged_at DESC;

CREATE OR REPLACE VIEW v_compliance_humans_only AS
SELECT repository,
       count(*)                                    AS prs,
       count(*) FILTER (WHERE is_compliant)        AS compliant_prs,
       round(avg(is_compliant::int)::numeric, 4)   AS compliance_rate
FROM pr_compliance
WHERE NOT is_bot_author
GROUP BY repository;

CREATE OR REPLACE VIEW v_compliance_daily AS
SELECT merged_at::date                                                   AS day,
       count(*)                                                          AS prs,
       round(avg(is_compliant::int)::numeric, 4)                         AS compliance_rate,
       round(avg(is_compliant::int) FILTER (WHERE NOT is_bot_author)::numeric, 4)
                                                                         AS human_compliance_rate
FROM pr_compliance
GROUP BY merged_at::date;
