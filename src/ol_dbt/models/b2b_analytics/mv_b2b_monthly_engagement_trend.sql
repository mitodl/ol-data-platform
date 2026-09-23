{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x year_month. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Note: organization_key is COALESCE(b2b_contract_to_courseruns.organization_key, user_course_roles.organization)
-- in the source report. The fallback is free-text and unreliable, so rows without a
-- confirmed organization_key are dropped -- acceptable for MVP.
-- sso_organization_id (the Keycloak org UUID the ol-analytics-api filters on) is
-- joined from dim_organization on organization_key; it is null for free-text
-- organization_keys that don't resolve to a known mitxonline org.
--
-- new_enrollments and certificates_earned count EVENTS (per learner per course
-- run), not learners: one learner enrolling in six runs reads as six. The
-- activity totals are likewise contributed to by only the learners who did that
-- specific thing, and every one of those cohorts is a strict subset of
-- monthly_active_learners (active_count is 1 on ANY activity, and enrolling
-- alone doesn't set it at all). ol-analytics-api can only apply its k-anonymity
-- floor to a cohort this view emits, so each aggregate publishes the distinct
-- learner count it is attributable to. Do not add an aggregate here without
-- also emitting its cohort.
select
    oar.organization_key,
    org.sso_organization_id,
    oar.organization_name,
    oar.activity_year_and_month,
    count(distinct case when oar.active_count > 0 then oar.user_email end)       as monthly_active_learners,
    sum(oar.enrolled_count)                                                      as new_enrollments,
    count(distinct case when oar.enrolled_count > 0 then oar.user_email end)     as enrolling_learners,
    sum(oar.certificate_count)                                                   as certificates_earned,
    count(distinct case when oar.certificate_count > 0 then oar.user_email end)  as certified_learners,
    sum(oar.videos_watched)                                                      as total_videos_watched,
    count(distinct case when oar.videos_watched > 0 then oar.user_email end)     as video_watchers,
    sum(oar.problems_count)                                                      as total_problems_attempted,
    count(distinct case when oar.problems_count > 0 then oar.user_email end)     as problem_attempters,
    sum(oar.chatbot_used_count)                                                  as total_chatbot_interactions,
    count(distinct case when oar.chatbot_used_count > 0 then oar.user_email end) as chatbot_users
from {{ source('reporting', 'organization_administration_report') }} oar
-- Dedupe to one row per organization_key so this join can never fan out and
-- inflate the aggregates. organization_key is unique per mitxonline org today
-- (source-enforced), so the group by is a no-op now but keeps the model correct
-- if that ever regresses.
left join (
    select
        organization_key,
        min(sso_organization_id) as sso_organization_id
    from {{ source('dimensional', 'dim_organization') }}
    where platform = 'mitxonline' and organization_key is not null
    group by organization_key
) org on oar.organization_key = org.organization_key
where oar.organization_key is not null
  and oar.activity_year_and_month is not null
group by
    oar.organization_key,
    org.sso_organization_id,
    oar.organization_name,
    oar.activity_year_and_month
