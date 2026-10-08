{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run (all-time). Refreshed by the Dagster
-- b2b_organization MV-refresh asset.
--
-- The contract-scoped sibling of mv_b2b_content_engagement_depth, which stays
-- at org x course_run. See that model and
-- mv_b2b_contract_monthly_engagement_trend for why both grains exist, for the
-- complement-disclosure consequence of publishing both, and for why contract
-- identity is emitted as both contract_pk (dimensional surrogate) and
-- contract_id (mitxonline's ContractPage.page_ptr_id, the one a caller
-- filters on).
--
-- Because a course run belongs to exactly one contract, this view's rows are a
-- strict partition of the org-level view's rows: adding the contract does not
-- split any course run, it only labels it. Every count here therefore equals
-- its org-level counterpart for the same course run -- which is precisely what
-- makes the complement recoverable when only some contracts are suppressed.
--
-- Every activity SUM here is contributed to by only the learners who did that
-- specific thing, and those cohorts are subsets of engaged_learners (enrolled
-- learners with at least one day of tracked course activity in the run).
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
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    cc.courserun_readable_id,
    cc.courserun_title,
    count(distinct lc.user_fk)                                                as total_enrolled_learners,
    count(distinct case when lc.days_active > 0 then lc.user_fk end)          as engaged_learners,
    round(100.0 * count(distinct case when lc.days_active > 0 then lc.user_fk end)
        / nullif(count(distinct lc.user_fk), 0), 1)                           as engagement_rate_pct,
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
        / nullif(count(distinct lc.user_fk), 0), 1)                           as chatbot_adoption_pct,
    sum(case when lc.is_certified then 1 else 0 end)                          as certificates_earned
from learner_courseruns lc
join contract_courseruns cc
    on lc.courserun_fk = cc.courserun_pk
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    cc.courserun_readable_id,
    cc.courserun_title
