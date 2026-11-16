{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run x learner, as in mv_b2b_learner_enrollment, plus
-- one row with null course run and enrollment columns for each member with no active
-- enrollment. Refreshed by the Dagster b2b_organization MV-refresh asset. Individually
-- identifying: backs the b2b_dashboard tenant's /learners, the MIT Learn Learners page,
-- which lists every current contract member with a status (mitodl/hq#14364). The
-- dashboard's counts and /learner-progress keep reading mv_b2b_learner_enrollment, so
-- they do not change.
-- Rows start from the contract roster: a learner whose membership is no longer current
-- drops off, while mv_b2b_learner_enrollment keeps their enrollments. Seats assigned to
-- an email but not yet redeemed have no membership, so they are not here.
-- Enrollments are active ones in current runs, which is all /learners reads. A member
-- whose enrollments are all inactive gets the member row.
-- A member with any enrollment under the contract (active or not) and no recorded
-- consent is left out, rows and all, until they agree: that is the group who enrolled
-- before the consent prompt, and leaving them out keeps the page fail-closed. A
-- recorded decline is left out the same way. A member with no enrollment is listed
-- whatever their consent, since there is no outcome to withhold.
-- MIT Learn keeps a learner out of the contract's courses until they consent
-- (ContractContent.tsx, behind the B2BDataConsent flag), so a member who declined has
-- no enrollment and is listed as not enrolled.
-- Built from the sources rather than from mv_b2b_learner_enrollment: the refresh asset
-- refreshes MVs in name order, which would refresh this one first.
with activity as (
    select
        user_fk,
        courserun_fk,
        count(distinct activity_date_key)                                               as days_active,
        sum(videos_played)                                                              as videos_played,
        sum(problems_attempted)                                                         as problems_attempted,
        sum(chatbot_interactions)                                                       as chatbot_interactions
    from {{ source('dimensional', 'afact_learner_courserun_daily_activity') }}
    where platform = 'mitxonline'
    group by user_fk, courserun_fk
),

-- Every enrollment, not afact_learner_courserun_progress: that keeps one per (user, run),
-- so an older enrollment under this contract can be missing from it.
contract_enrollments as (
    select distinct user_fk, contract_fk
    from {{ source('dimensional', 'tfact_enrollment') }}
    where contract_fk is not null
),

members as (
    select
        uc.user_fk,
        uc.contract_fk,
        uc.consented_to_data_sharing
    from {{ source('dimensional', 'bridge_user_contract') }} uc
    left join contract_enrollments ce
        on uc.user_fk = ce.user_fk and uc.contract_fk = ce.contract_fk
    where uc.membership_is_current
      and (uc.consented_to_data_sharing or ce.user_fk is null)
),

enrollments as (
    select
        p.user_fk,
        p.contract_fk,
        cr.courserun_pk,
        cr.courserun_readable_id,
        cr.courserun_title,
        cr.courserun_start_on,
        cr.courserun_end_on,
        p.enrollment_created_on,
        p.enrollment_is_active,
        p.enrollment_mode,
        p.is_passing,
        p.grade_value,
        p.letter_grade,
        p.certificate_issued_on,
        p.certificate_is_revoked,
        p.last_active_on,
        p.completion_status,
        p.needs_attention_since,
        p.content_progress,
        coalesce(a.days_active, 0)                                                      as days_active,
        coalesce(a.videos_played, 0)                                                    as videos_played,
        coalesce(a.problems_attempted, 0)                                               as problems_attempted,
        coalesce(a.chatbot_interactions, 0)                                             as chatbot_interactions
    from {{ source('dimensional', 'afact_learner_courserun_progress') }} p
    join {{ source('dimensional', 'dim_course_run') }} cr
        on p.courserun_fk = cr.courserun_pk
    left join activity a
        on p.user_fk = a.user_fk and p.courserun_fk = a.courserun_fk
    where p.enrollment_is_active = true
      and cr.is_current = true
)

select
    org.organization_key,
    org.sso_organization_id,
    c.contract_pk,
    c.contract_id,
    e.courserun_pk,
    e.courserun_readable_id,
    e.courserun_title,
    e.courserun_start_on,
    e.courserun_end_on,
    u.user_pk,
    u.user_global_id,
    u.email,
    -- dim_user falls back to first + ' ' + last, which reads ' ' when both are empty.
    nullif(trim(u.full_name), '')                                                       as full_name,
    e.enrollment_created_on,
    e.enrollment_is_active,
    e.enrollment_mode,
    e.is_passing,
    e.grade_value,
    e.letter_grade,
    e.certificate_issued_on,
    e.certificate_is_revoked,
    e.last_active_on,
    e.completion_status,
    e.needs_attention_since,
    e.content_progress,
    e.days_active,
    e.videos_played,
    e.problems_attempted,
    e.chatbot_interactions,
    m.consented_to_data_sharing                                                         as outcomes_shared
from members m
join {{ source('dimensional', 'dim_contract') }} c
    on m.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_user') }} u
    on m.user_fk = u.user_pk
left join enrollments e
    on m.user_fk = e.user_fk and m.contract_fk = e.contract_fk
where org.platform = 'mitxonline'
  -- The API's learner_id is required; see mv_b2b_learner_enrollment.
  and u.user_global_id is not null
