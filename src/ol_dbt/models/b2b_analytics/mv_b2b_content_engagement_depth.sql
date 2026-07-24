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
    round(
        cast(sum(oar.videos_watched) as double)
        / nullif(count(distinct case when oar.videos_watched > 0
            then oar.user_email end), 0), 1
    )                                                                           as avg_videos_per_engaged_learner,
    sum(oar.problems_count)                                                     as total_problems_attempted,
    round(
        cast(sum(oar.problems_count) as double)
        / nullif(count(distinct case when oar.problems_count > 0
            then oar.user_email end), 0), 1
    )                                                                           as avg_problems_per_engaged_learner,
    sum(oar.chatbot_used_count)                                                 as total_chatbot_interactions,
    count(distinct case when oar.chatbot_used_count > 0 then oar.user_email end) as chatbot_users,
    round(100.0 * count(distinct case when oar.chatbot_used_count > 0
        then oar.user_email end)
        / nullif(count(distinct oar.user_email), 0), 1)                         as chatbot_adoption_pct,
    sum(oar.certificate_count)                                                  as certificates_earned
from {{ source('reporting', 'organization_administration_report') }} oar
left join {{ source('dimensional', 'dim_organization') }} org
    on oar.organization_key = org.organization_key
    and org.platform = 'mitxonline'
where oar.organization_key is not null
group by
    oar.organization_key,
    org.sso_organization_id,
    oar.organization_name,
    oar.courserun_readable_id,
    oar.courserun_title
