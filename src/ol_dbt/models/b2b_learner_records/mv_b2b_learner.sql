{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x learner. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Individually identifying: read by ol-analytics-api's b2b_learner_records tenant only.
-- Rolled up from the same joins as mv_b2b_learner_enrollment rather than selected
-- from it: the refresh asset refreshes MVs in name order, which would refresh this
-- one before the view it reads.
-- Enrollment rollups count ACTIVE enrollments only, matching the API's default
-- include_inactive=false. A contract_id or include_inactive request is recomputed
-- from mv_b2b_learner_enrollment.
with active_enrollments as (
    select
        c.organization_fk,
        e.user_fk,
        e.courserun_fk,
        cr.course_fk,
        e.enrollment_created_on,
        g.is_passing,
        cert.certificate_is_revoked,
        greatest(
            coalesce(e.enrollment_updated_on, e.enrollment_created_on),
            coalesce(g.grade_updated_on, e.enrollment_created_on),
            coalesce(cert.certificate_updated_on, e.enrollment_created_on)
        ) as record_updated_on
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'dim_contract') }} c
        on boc.contract_fk = c.contract_pk
    join {{ source('dimensional', 'dim_course_run') }} cr
        on boc.courserun_fk = cr.courserun_pk
    join {{ source('dimensional', 'tfact_enrollment') }} e
        on boc.courserun_fk = e.courserun_fk
    left join {{ source('dimensional', 'tfact_grade') }} g
        on e.user_fk = g.user_fk and e.courserun_fk = g.courserun_fk
    left join {{ source('dimensional', 'tfact_certificate') }} cert
        on e.user_fk = cert.user_fk and e.courserun_fk = cert.courserun_fk
    where cr.is_current = true
      and e.enrollment_is_active = true
      and e.user_fk is not null
),

enrollment_rollup as (
    select
        organization_fk,
        user_fk,
        min(enrollment_created_on)                                                      as first_enrolled_on,
        max(enrollment_created_on)                                                      as last_enrolled_on,
        count(distinct courserun_fk)                                                    as courses_enrolled,
        count(distinct case when is_passing then courserun_fk end)                      as courses_passed,
        count(distinct case when certificate_is_revoked = false
            then courserun_fk end)                                                      as courses_certified,
        max(record_updated_on)                                                          as record_updated_on
    from active_enrollments
    group by organization_fk, user_fk
),

-- A program certificate has no course run, so it is attributed to the organization
-- only when the program contains a course the learner is actively enrolled in under
-- one of the organization's contracts. Without that bound a certificate earned
-- privately, before the licence, would be reported to the partner.
program_certificates as (
    select
        ae.organization_fk,
        ae.user_fk,
        count(distinct cert.program_fk)                                                 as program_certificates_earned,
        max(cert.certificate_updated_on)                                                as program_certificate_updated_on
    from active_enrollments ae
    join {{ source('dimensional', 'bridge_program_course') }} bpc
        on ae.course_fk = bpc.course_fk
    join {{ source('dimensional', 'tfact_certificate') }} cert
        on ae.user_fk = cert.user_fk
        and bpc.program_fk = cert.program_fk
    where cert.certificate_scope = 'program'
      and cert.platform = 'mitxonline'
      and cert.certificate_is_revoked = false
    group by ae.organization_fk, ae.user_fk
),

-- Roster and enrollment legitimately disagree: a roster row with no enrollment is an
-- assigned, unstarted seat; an enrollment with no roster row is usually provisioning
-- lag. Union rather than pick one, so both surface as membership_source.
memberships as (
    select
        organization_fk,
        user_fk,
        max(on_roster)                                                                  as on_roster,
        max(is_enrolled)                                                                as is_enrolled,
        max(is_manager)                                                                 as is_manager
    from (
        select
            organization_fk,
            user_fk,
            1                                                                           as on_roster,
            0                                                                           as is_enrolled,
            case when userorganization_is_manager then 1 else 0 end                     as is_manager
        from {{ source('dimensional', 'bridge_user_organization') }}
        union all
        select
            organization_fk,
            user_fk,
            0                                                                           as on_roster,
            1                                                                           as is_enrolled,
            0                                                                           as is_manager
        from enrollment_rollup
    ) m
    group by organization_fk, user_fk
)

select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    u.user_pk,
    u.user_global_id,
    u.email,
    u.full_name,
    case
        when m.on_roster = 1 and m.is_enrolled = 1 then 'both'
        when m.on_roster = 1                        then 'roster'
        else 'enrollment'
    end                                                                                 as membership_source,
    m.is_manager = 1                                                                    as is_organization_manager,
    er.first_enrolled_on,
    er.last_enrolled_on,
    coalesce(er.courses_enrolled, 0)                                                    as courses_enrolled,
    coalesce(er.courses_passed, 0)                                                      as courses_passed,
    coalesce(er.courses_certified, 0)                                                   as courses_certified,
    coalesce(pc.program_certificates_earned, 0)                                         as program_certificates_earned,
    -- A program certificate row exists only alongside an active enrollment, so
    -- er.record_updated_on is non-null whenever pc is; the coalesce only keeps
    -- greatest() from going null when there is no program certificate.
    greatest(
        er.record_updated_on,
        coalesce(pc.program_certificate_updated_on, er.record_updated_on)
    )                                                                                   as record_updated_on
from memberships m
join {{ source('dimensional', 'dim_organization') }} org
    on m.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_user') }} u
    on m.user_fk = u.user_pk
left join enrollment_rollup er
    on m.organization_fk = er.organization_fk and m.user_fk = er.user_fk
left join program_certificates pc
    on m.organization_fk = pc.organization_fk and m.user_fk = pc.user_fk
where org.platform = 'mitxonline'
