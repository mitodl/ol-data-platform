{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x course_run (all-time). Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Rows come from the dimensional facts through the contract that owns the course run
-- (see macros/b2b_engagement.sql); see mv_b2b_monthly_engagement_trend for what that
-- changed.
--
-- engaged_learners counts enrolled learners with at least one day of tracked course
-- activity in the run. Every activity SUM here is contributed to by only the learners
-- who did that specific thing, and those cohorts are subsets of engaged_learners.
-- ol-analytics-api can only apply its k-anonymity floor to a cohort this view
-- emits, so each such sum publishes its own contributing cohort count
-- (video_watchers, problem_attempters, chatbot_users) alongside it. Do not add
-- an activity aggregate without also emitting the cohort it is attributable to.
with contract_courseruns as (
{{ b2b_contract_courseruns() }}
)

, learner_courseruns as (
{{ b2b_learner_courserun_engagement() }}
)

select
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.courserun_readable_id,
    cc.courserun_title,
    count(distinct case when lc.enrollment_is_active then lc.user_fk end)     as total_enrolled_learners,
    count(distinct case when lc.days_active > 0 then lc.user_fk end)          as engaged_learners,
    round(100.0 * count(distinct case when lc.days_active > 0 then lc.user_fk end)
        / nullif(count(distinct case when lc.enrollment_is_active then lc.user_fk end), 0), 1
    )                                                                         as engagement_rate_pct,
    sum(lc.videos_played)                                                     as total_videos_watched,
    count(distinct case when lc.videos_played > 0 then lc.user_fk end)        as video_watchers,
    round(
        cast(sum(lc.videos_played) as double)
        / nullif(count(distinct case when lc.days_active > 0 then lc.user_fk end), 0), 1
    )                                                                         as avg_videos_per_engaged_learner,
    sum(lc.problems_attempted)                                                as total_problems_attempted,
    count(distinct case when lc.problems_attempted > 0 then lc.user_fk end)   as problem_attempters,
    round(
        cast(sum(lc.problems_attempted) as double)
        / nullif(count(distinct case when lc.days_active > 0 then lc.user_fk end), 0), 1
    )                                                                         as avg_problems_per_engaged_learner,
    sum(lc.chatbot_interactions)                                              as total_chatbot_interactions,
    count(distinct case when lc.chatbot_interactions > 0 then lc.user_fk end) as chatbot_users,
    round(100.0 * count(distinct case when lc.chatbot_interactions > 0 then lc.user_fk end)
        / nullif(count(distinct case when lc.enrollment_is_active then lc.user_fk end), 0), 1
    )                                                                         as chatbot_adoption_pct,
    sum(case when lc.is_certified then 1 else 0 end)                          as certificates_earned
from learner_courseruns lc
join contract_courseruns cc
    on lc.courserun_fk = cc.courserun_pk
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.courserun_readable_id,
    cc.courserun_title
