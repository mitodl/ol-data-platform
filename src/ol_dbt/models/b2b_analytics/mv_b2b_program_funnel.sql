{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x program. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- program_course_completers is an approximation (cert in any program course, not a true
-- program-level certificate) until tfact_program_certificate (dimensional todo C2) ships.
with org_program_runs as (
    -- Resolve org -> contract -> course_run -> course -> program
    select distinct
        org.organization_key,
        org.organization_name,
        c.contract_pk,
        c.b2b_contract_name,
        bpc.program_fk,
        boc.courserun_fk
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'dim_contract') }} c
        on boc.contract_fk = c.contract_pk
    join {{ source('dimensional', 'dim_organization') }} org
        on c.organization_fk = org.organization_pk
    join {{ source('dimensional', 'dim_course_run') }} cr
        on boc.courserun_fk = cr.courserun_pk and cr.is_current = true
    join {{ source('dimensional', 'bridge_program_course') }} bpc
        on cr.course_fk = bpc.course_fk
    where org.platform = 'mitxonline'
),

program_course_counts as (
    select program_fk, count(distinct course_fk) as total_courses
    from {{ source('dimensional', 'bridge_program_course') }}
    group by program_fk
)

select
    opr.organization_key,
    opr.organization_name,
    opr.contract_pk,
    opr.b2b_contract_name,
    p.program_pk,
    p.program_title,
    pcc.total_courses,
    -- Learners enrolled in any contract course that belongs to this program
    count(distinct e.user_fk)                                                     as enrolled_in_contract_courses,
    -- Learners who explicitly enrolled via the program pathway
    count(distinct case when e.program_fk = p.program_pk then e.user_fk end)      as enrolled_via_program,
    count(distinct case when cert.certificate_is_revoked = false
        then cert.user_fk end)                                                    as program_course_completers
from org_program_runs opr
join {{ source('dimensional', 'dim_program') }} p
    on opr.program_fk = p.program_pk
join program_course_counts pcc
    on opr.program_fk = pcc.program_fk
left join {{ source('dimensional', 'tfact_enrollment') }} e
    on opr.courserun_fk = e.courserun_fk
left join {{ source('dimensional', 'tfact_certificate') }} cert
    on e.user_fk = cert.user_fk and opr.courserun_fk = cert.courserun_fk
group by
    opr.organization_key, opr.organization_name,
    opr.contract_pk, opr.b2b_contract_name,
    p.program_pk, p.program_title,
    pcc.total_courses
