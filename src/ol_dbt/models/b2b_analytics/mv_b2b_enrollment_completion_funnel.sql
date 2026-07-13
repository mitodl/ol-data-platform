{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run. Refreshed by the Dagster b2b_organization MV-refresh asset.
select
    org.organization_key,
    org.organization_name,
    c.contract_pk,
    c.b2b_contract_name,
    cr.courserun_pk,
    cr.courserun_readable_id,
    cr.courserun_title,
    count(distinct e.user_fk)                                                           as enrolled_learners,
    count(distinct case when e.enrollment_is_active then e.user_fk end)                 as active_learners,
    count(distinct case when g.is_passing then g.user_fk end)                           as passing_learners,
    count(distinct case when cert.certificate_is_revoked = false
        then cert.user_fk end)                                                          as certified_learners,
    round(100.0 * count(distinct case when e.enrollment_is_active then e.user_fk end)
        / nullif(count(distinct e.user_fk), 0), 1)                                     as active_rate_pct,
    round(100.0 * count(distinct case when cert.certificate_is_revoked = false
        then cert.user_fk end)
        / nullif(count(distinct e.user_fk), 0), 1)                                     as completion_rate_pct
from {{ source('dimensional', 'bridge_organization_courserun') }} boc
join {{ source('dimensional', 'dim_contract') }} c
    on boc.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_course_run') }} cr
    on boc.courserun_fk = cr.courserun_pk
join {{ source('dimensional', 'tfact_enrollment') }} e
    on boc.courserun_fk = e.courserun_fk
left join {{ source('dimensional', 'tfact_grade') }} g
    on e.user_fk = g.user_fk and e.courserun_fk = g.courserun_fk
left join {{ source('dimensional', 'tfact_certificate') }} cert
    on e.user_fk = cert.user_fk and e.courserun_fk = cert.courserun_fk
where org.platform = 'mitxonline'
  and cr.is_current = true
group by
    org.organization_key, org.organization_name,
    c.contract_pk, c.b2b_contract_name,
    cr.courserun_pk, cr.courserun_readable_id, cr.courserun_title
