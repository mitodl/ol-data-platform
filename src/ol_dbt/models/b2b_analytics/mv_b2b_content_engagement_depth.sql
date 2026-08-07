{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x course_run (all-time). Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Same organization_key reliability caveat as mv_b2b_monthly_engagement_trend.
-- sso_organization_id (the Keycloak org UUID the ol-analytics-api filters on) is
-- joined from dim_organization on organization_key; null for free-text
-- organization_keys that don't resolve to a known mitxonline org.
--
-- Every activity SUM here is contributed to by only the learners who did that
-- specific thing, and those cohorts are strict subsets of engaged_learners
-- (active_count is 1 on ANY activity -- see organization_administration_report).
-- ol-analytics-api can only apply its k-anonymity floor to a cohort this view
-- emits, so each such sum publishes its own contributing cohort count
-- (video_watchers, problem_attempters, chatbot_users) alongside it. Do not add
-- an activity aggregate without also emitting the cohort it is attributable to.
select
    oar.organization_key,
    org.sso_organization_id,
    oar.organization_name,
    oar.courserun_readable_id,
    oar.courserun_title,
    count(distinct oar.user_email)                                              as total_enrolled_learners,
    count(distinct case when oar.active_count > 0 then oar.user_email end)      as engaged_learners,
    round(100.0 * count(distinct case when oar.active_count > 0 then oar.user_email end)
        / nullif(count(distinct oar.user_email), 0), 1)                         as engagement_rate_pct,
    sum(oar.videos_watched)                                                     as total_videos_watched,
    count(distinct case when oar.videos_watched > 0 then oar.user_email end)    as video_watchers,
    round(
        cast(sum(oar.videos_watched) as double)
        / nullif(count(distinct case when oar.active_count > 0
            then oar.user_email end), 0), 1
    )                                                                           as avg_videos_per_engaged_learner,
    sum(oar.problems_count)                                                     as total_problems_attempted,
    count(distinct case when oar.problems_count > 0 then oar.user_email end)    as problem_attempters,
    round(
        cast(sum(oar.problems_count) as double)
        / nullif(count(distinct case when oar.active_count > 0
            then oar.user_email end), 0), 1
    )                                                                           as avg_problems_per_engaged_learner,
    sum(oar.chatbot_used_count)                                                 as total_chatbot_interactions,
    count(distinct case when oar.chatbot_used_count > 0 then oar.user_email end) as chatbot_users,
    round(100.0 * count(distinct case when oar.chatbot_used_count > 0
        then oar.user_email end)
        / nullif(count(distinct oar.user_email), 0), 1)                         as chatbot_adoption_pct,
    sum(oar.certificate_count)                                                  as certificates_earned
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
group by
    oar.organization_key,
    org.sso_organization_id,
    oar.organization_name,
    oar.courserun_readable_id,
    oar.courserun_title
