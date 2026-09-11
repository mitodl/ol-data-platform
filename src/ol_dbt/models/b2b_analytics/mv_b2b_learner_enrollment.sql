{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run x learner. Refreshed by the Dagster b2b_organization
-- MV-refresh asset. Individually identifying: read by ol-analytics-api's
-- b2b_learner_records tenant, never by the k-anonymized b2b_dashboard tenant.
-- The organization is reached through the contract that owns the course run, never
-- organization_administration_report's free-text organization_key fallback, which
-- would show a partner learners who aren't theirs.
select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    c.contract_pk,
    c.contract_id,
    c.b2b_contract_name,
    cr.courserun_pk,
    cr.courserun_readable_id,
    cr.courserun_title,
    cr.courserun_start_on,
    cr.courserun_end_on,
    u.user_pk,
    u.user_global_id,
    u.email,
    u.full_name,
    e.enrollment_created_on,
    e.enrollment_is_active,
    e.enrollment_mode,
    e.enrollment_status,
    g.is_passing,
    g.grade_value,
    g.letter_grade,
    cert.certificate_issued_on,
    cert.certificate_is_revoked,
    -- Every mitxonline timestamp here is an ISO-8601 string from the same macro, so
    -- greatest() compares them correctly. Grade and certificate floor to the
    -- enrollment's own timestamp when absent.
    greatest(
        coalesce(e.enrollment_updated_on, e.enrollment_created_on),
        coalesce(g.grade_updated_on, e.enrollment_created_on),
        coalesce(cert.certificate_updated_on, e.enrollment_created_on)
    )                                                                                   as record_updated_on
from {{ source('dimensional', 'bridge_organization_courserun') }} boc
join {{ source('dimensional', 'dim_contract') }} c
    on boc.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_course_run') }} cr
    on boc.courserun_fk = cr.courserun_pk
join {{ source('dimensional', 'tfact_enrollment') }} e
    on boc.courserun_fk = e.courserun_fk
join {{ source('dimensional', 'dim_user') }} u
    on e.user_fk = u.user_pk
left join {{ source('dimensional', 'tfact_grade') }} g
    on e.user_fk = g.user_fk and e.courserun_fk = g.courserun_fk
left join {{ source('dimensional', 'tfact_certificate') }} cert
    on e.user_fk = cert.user_fk and e.courserun_fk = cert.courserun_fk
where org.platform = 'mitxonline'
  and cr.is_current = true
