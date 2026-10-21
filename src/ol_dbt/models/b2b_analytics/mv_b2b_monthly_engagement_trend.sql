{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x year_month. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Rows come from the dimensional facts through the contract each learner enrolled under
-- (see macros/b2b_engagement.sql), the same learners and counters the learner-records
-- views publish. Before this it read organization_administration_report, which keys
-- learners on email, counts showanswer as a problem attempt and counts the day a
-- certificate is issued as activity.
--
-- monthly_active_learners counts learners with tracked course activity in the month.
-- Enrolling or being issued a certificate in a month does not make a learner active
-- in it.
--
-- new_enrollments and certificates_earned count EVENTS (per learner per course
-- run), not learners: one learner enrolling in six runs reads as six. The
-- activity totals are likewise contributed to by only the learners who did that
-- specific thing, and each of those cohorts is a subset of
-- monthly_active_learners. ol-analytics-api can only apply its k-anonymity
-- floor to a cohort this view emits, so each aggregate publishes the distinct
-- learner count it is attributable to. Do not add an aggregate here without
-- also emitting its cohort.
--
-- contributing_learners counts every learner behind the row: active, enrolling
-- or certified in the month. It is the cohort the API gates the whole row on,
-- because enrolling_learners and newly_certified_learners are not subsets of
-- monthly_active_learners and a month can have enough of either without enough
-- active learners.
--
-- certified_learners is the old name for newly_certified_learners, kept until
-- ol-analytics-api and mit-learn read the new one. The name now belongs to the
-- certified_learners metric (learners holding an unrevoked certificate for a run),
-- which is what the contract health, funnel and utilization views publish.
with contracts as (
{{ b2b_contracts() }}
)

, learner_months as (
{{ b2b_learner_courserun_months() }}
)

select
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    lm.activity_year_and_month,
    count(distinct case when lm.is_active_day > 0 then lm.user_fk end)        as monthly_active_learners,
    sum(lm.new_enrollments)                                                   as new_enrollments,
    count(distinct case when lm.new_enrollments > 0 then lm.user_fk end)      as enrolling_learners,
    sum(lm.certificates_earned)                                               as certificates_earned,
    count(distinct case when lm.certificates_earned > 0 then lm.user_fk end)  as certified_learners,
    count(distinct case when lm.certificates_earned > 0 then lm.user_fk end)  as newly_certified_learners,
    sum(lm.videos_played)                                                     as total_videos_watched,
    count(distinct case when lm.videos_played > 0 then lm.user_fk end)        as video_watchers,
    sum(lm.problems_attempted)                                                as total_problems_attempted,
    count(distinct case when lm.problems_attempted > 0 then lm.user_fk end)   as problem_attempters,
    sum(lm.chatbot_interactions)                                              as total_chatbot_interactions,
    count(distinct case when lm.chatbot_interactions > 0 then lm.user_fk end) as chatbot_users,
    count(distinct lm.user_fk)                                                as contributing_learners
from learner_months lm
join contracts cc
    on lm.contract_fk = cc.contract_pk
-- A row with no month is not a month the API can publish.
where lm.activity_year_and_month is not null
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    lm.activity_year_and_month
