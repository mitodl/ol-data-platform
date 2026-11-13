{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run. Refreshed by the Dagster b2b_organization
-- MV-refresh asset. Backs ol-analytics-api's b2b_learner_records /courses collection.
-- No personal data, but kept in this database so the tenant reads one schema.
-- Not derived from mv_b2b_learner_enrollment: that view inner-joins the enrollments,
-- so a contract run nobody has enrolled in yet would be missing from /courses.
select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    c.contract_pk,
    c.contract_id,
    c.b2b_contract_name,
    c.b2b_contract_is_active,
    c.b2b_contract_start_date,
    c.b2b_contract_end_date,
    -- MITx Online documents max_learners 0 and null both as unlimited. The API
    -- contract reserves null for uncapped, so 0 is folded into it.
    nullif(c.b2b_contract_max_learners, 0)                                              as seat_limit,
    cr.courserun_pk,
    cr.courserun_readable_id,
    cr.courserun_title,
    cr.courserun_start_on,
    cr.courserun_end_on,
    bcc.program_sort_order,
    bcc.course_order_in_program
from {{ source('dimensional', 'bridge_organization_courserun') }} boc
join {{ source('dimensional', 'dim_contract') }} c
    on boc.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_course_run') }} cr
    on boc.courserun_fk = cr.courserun_pk
-- One row per (contract, course), so the join keeps the view's grain.
left join {{ source('dimensional', 'bridge_contract_course') }} bcc
    on boc.contract_fk = bcc.contract_fk
    and cr.course_fk = bcc.course_fk
where org.platform = 'mitxonline'
  and cr.is_current = true
