{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x course_run (all-time). Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Same organization_key reliability caveat as mv_b2b_monthly_engagement_trend.
select
    organization_key,
    organization_name,
    courserun_readable_id,
    courserun_title,
    count(distinct user_email)                                                 as total_enrolled_learners,
    count(distinct case when active_count > 0 then user_email end)             as engaged_learners,
    round(100.0 * count(distinct case when active_count > 0 then user_email end)
        / nullif(count(distinct user_email), 0), 1)                            as engagement_rate_pct,
    sum(videos_watched)                                                        as total_videos_watched,
    round(
        cast(sum(videos_watched) as double)
        / nullif(count(distinct case when videos_watched > 0
            then user_email end), 0), 1
    )                                                                          as avg_videos_per_engaged_learner,
    sum(problems_count)                                                        as total_problems_attempted,
    round(
        cast(sum(problems_count) as double)
        / nullif(count(distinct case when problems_count > 0
            then user_email end), 0), 1
    )                                                                          as avg_problems_per_engaged_learner,
    sum(chatbot_used_count)                                                    as total_chatbot_interactions,
    count(distinct case when chatbot_used_count > 0 then user_email end)       as chatbot_users,
    round(100.0 * count(distinct case when chatbot_used_count > 0
        then user_email end)
        / nullif(count(distinct user_email), 0), 1)                            as chatbot_adoption_pct,
    sum(certificate_count)                                                     as certificates_earned
from {{ source('reporting', 'organization_administration_report') }}
where organization_key is not null
group by
    organization_key,
    organization_name,
    courserun_readable_id,
    courserun_title
