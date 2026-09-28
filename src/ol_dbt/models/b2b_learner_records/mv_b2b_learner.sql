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
-- courses_enrolled and the enrolled_on dates count ACTIVE enrollments, matching the
-- API's default include_inactive=false. Completions count every enrollment under the
-- organization's contracts, so reclaiming a seat does not take back a completion a
-- partner was already sent. A contract_id or include_inactive request is recomputed
-- from mv_b2b_learner_enrollment.
-- last_active_on and courses_in_progress also count active enrollments only: they
-- describe current engagement, not an outcome already sent. Activity does not move
-- record_updated_on; see mv_b2b_learner_enrollment.
-- Pre-aggregated to the (user, course run) join key so it cannot fan out.
with activity as (
    select
        user_fk,
        courserun_fk,
        max(activity_date_key)                                                          as last_active_date_key
    from {{ source('dimensional', 'afact_learner_courserun_daily_activity') }}
    where platform = 'mitxonline'
    group by user_fk, courserun_fk
),

contract_enrollments as (
    select
        c.organization_fk,
        e.user_fk,
        e.courserun_fk,
        cr.course_fk,
        e.enrollment_created_on,
        e.enrollment_is_active,
        g.is_passing,
        g.grade_value,
        cert.certificate_is_revoked,
        a.last_active_date_key,
        -- StarRocks' greatest() returns null if any argument is null; '' sorts below
        -- any ISO-8601 date.
        greatest(
            coalesce(e.enrollment_updated_on, ''),
            coalesce(e.enrollment_created_on, ''),
            coalesce(g.grade_updated_on, ''),
            coalesce(cert.certificate_updated_on, '')
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
    left join activity a
        on e.user_fk = a.user_fk and e.courserun_fk = a.courserun_fk
    where cr.is_current = true
      and e.user_fk is not null
),

-- record_updated_on is the max over every enrollment, not just the ones a counter
-- keeps. Deactivating an enrollment or revoking a certificate is a save() upstream that
-- moves its updated_on; a max over the filtered set would move backwards instead.
enrollment_rollup as (
    select
        organization_fk,
        user_fk,
        min(case when enrollment_is_active then enrollment_created_on end)              as first_enrolled_on,
        max(case when enrollment_is_active then enrollment_created_on end)              as last_enrolled_on,
        count(distinct case when enrollment_is_active then courserun_fk end)            as courses_enrolled,
        count(distinct case when is_passing then courserun_fk end)                      as courses_passed,
        count(distinct case when certificate_is_revoked = false
            then courserun_fk end)                                                      as courses_certified,
        max(case when enrollment_is_active then last_active_date_key end)               as last_active_date_key,
        -- The API's completion_status = in_progress, restricted to active enrollments:
        -- no unrevoked certificate, not passing, and a nonzero grade or any activity.
        count(distinct case when enrollment_is_active
            and coalesce(certificate_is_revoked, true)
            and not coalesce(is_passing, false)
            and (grade_value > 0 or last_active_date_key is not null)
            then courserun_fk end)                                                      as courses_in_progress,
        max(record_updated_on)                                                          as record_updated_on
    from contract_enrollments
    group by organization_fk, user_fk
),

-- A program certificate has no course run, so it is attributed to the organization
-- only when the program contains a course the learner enrolled in under one of the
-- organization's contracts. Without that bound a certificate earned privately, before
-- the licence, would be reported to the partner. Revoked certificates still move
-- program_certificate_updated_on so the revocation reaches updated_since.
program_certificates as (
    select
        ce.organization_fk,
        ce.user_fk,
        count(distinct case when cert.certificate_is_revoked = false
            then cert.program_fk end)                                                   as program_certificates_earned,
        max(coalesce(cert.certificate_updated_on, ''))                                  as program_certificate_updated_on
    from contract_enrollments ce
    join {{ source('dimensional', 'bridge_program_course') }} bpc
        on ce.course_fk = bpc.course_fk
    join {{ source('dimensional', 'tfact_certificate') }} cert
        on ce.user_fk = cert.user_fk
        and bpc.program_fk = cert.program_fk
    where cert.certificate_scope = 'program'
      and cert.platform = 'mitxonline'
    group by ce.organization_fk, ce.user_fk
),

-- Roster and enrollment legitimately disagree. A roster row with no active enrollment
-- is an assigned, unstarted seat. An enrollment with no roster row is a learner removed
-- from the organization who kept the enrollment, or an enrollment from before
-- mitxonline created a membership on enrollment. Union rather than pick one, so both
-- surface as membership_source; snapshot_mitxonline_b2b_userorganization records when
-- a membership was removed. Roster rows with keep_until_seen are included: mitxonline
-- sets it on memberships it creates itself (code redemption, contract enrollment), not
-- on memberships pending removal.
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
            case when courses_enrolled > 0 then 1 else 0 end                            as is_enrolled,
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
    cast(d.date as date)                                                                as last_active_on,
    coalesce(er.courses_in_progress, 0)                                                 as courses_in_progress,
    nullif(greatest(
        coalesce(er.record_updated_on, ''),
        coalesce(pc.program_certificate_updated_on, '')
    ), '')                                                                              as record_updated_on
from memberships m
join {{ source('dimensional', 'dim_organization') }} org
    on m.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_user') }} u
    on m.user_fk = u.user_pk
left join enrollment_rollup er
    on m.organization_fk = er.organization_fk and m.user_fk = er.user_fk
left join program_certificates pc
    on m.organization_fk = pc.organization_fk and m.user_fk = pc.user_fk
left join {{ source('dimensional', 'dim_date') }} d
    on er.last_active_date_key = d.date_key
where org.platform = 'mitxonline'
  -- The API's learner_id is required; see mv_b2b_learner_enrollment.
  and u.user_global_id is not null
