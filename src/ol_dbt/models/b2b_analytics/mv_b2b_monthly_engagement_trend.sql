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
select
    organization_key,
    organization_name,
    activity_year_and_month,
    count(distinct case when active_count > 0 then user_email end)  as monthly_active_learners,
    sum(enrolled_count)                                              as new_enrollments,
    sum(certificate_count)                                           as certificates_earned,
    sum(videos_watched)                                              as total_videos_watched,
    sum(problems_count)                                              as total_problems_attempted,
    sum(chatbot_used_count)                                          as total_chatbot_interactions
from {{ source('reporting', 'organization_administration_report') }}
where organization_key is not null
  and activity_year_and_month is not null
group by
    organization_key,
    organization_name,
    activity_year_and_month
