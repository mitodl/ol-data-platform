{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x year_month. Refreshed by the Dagster b2b_organization MV-refresh asset.
--
-- The contract-scoped sibling of mv_b2b_monthly_engagement_trend, which stays
-- at org x year_month. Both exist because the MIT Learn dashboard mirrors
-- mitxonline's manager dashboard, which is nested under
-- manager/organizations/{org}/contracts/{contract} -- while the org-level
-- panels remain in service.
--
-- DISCLOSURE NOTE. Publishing the same learners at two grains makes the
-- complement recoverable: an org's contracts sum to its org row, so a contract
-- suppressed by ol-analytics-api's k-anonymity floor can be recovered as
-- org_total - (the other contracts). That is inert while an org has one
-- contract (the contract row IS the org row) and becomes live at two or more.
-- Tracked as the complement-disclosure work in ol-analytics-api; do not treat
-- the per-column floor alone as sufficient once orgs hold multiple contracts.
--
-- Contract identity is published as BOTH keys on purpose. contract_pk is the
-- dimensional surrogate (md5 of the natural key) that joins to dim_contract;
-- contract_id is mitxonline's ContractPage.page_ptr_id, which is what appears
-- in that dashboard's URLs and therefore what a caller filters on. Emitting
-- only the surrogate is what left the API unable to scope to a contract.
--
-- new_enrollments and certificates_earned count EVENTS (per learner per course
-- run), not learners. The activity totals are likewise contributed to by only
-- the learners who did that specific thing. ol-analytics-api can only apply its
-- k-anonymity floor to a cohort this view emits, so each aggregate publishes
-- the distinct learner count it is attributable to. Do not add an aggregate
-- here without also emitting its cohort.
--
-- contributing_learners counts every learner behind the row (active, enrolling
-- or certified in the month) and is the cohort the API gates the whole row on.
--
-- A learner is counted under the contract that owns the course run the
-- activity happened in, so a learner active under two contracts contributes to
-- both rows -- the contract rows therefore do not partition the org's learner
-- count, and summing monthly_active_learners across contracts can exceed the
-- org row.
with contract_courseruns as (
{{ b2b_contract_courseruns() }}
)

, learner_months as (
{{ b2b_learner_courserun_months() }}
)

select
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    lm.activity_year_and_month,
    count(distinct case when lm.is_active_day > 0 then lm.user_fk end)        as monthly_active_learners,
    sum(lm.new_enrollments)                                                   as new_enrollments,
    count(distinct case when lm.new_enrollments > 0 then lm.user_fk end)      as enrolling_learners,
    sum(lm.certificates_earned)                                               as certificates_earned,
    count(distinct case when lm.certificates_earned > 0 then lm.user_fk end)  as certified_learners,
    sum(lm.videos_played)                                                     as total_videos_watched,
    count(distinct case when lm.videos_played > 0 then lm.user_fk end)        as video_watchers,
    sum(lm.problems_attempted)                                                as total_problems_attempted,
    count(distinct case when lm.problems_attempted > 0 then lm.user_fk end)   as problem_attempters,
    sum(lm.chatbot_interactions)                                              as total_chatbot_interactions,
    count(distinct case when lm.chatbot_interactions > 0 then lm.user_fk end) as chatbot_users,
    count(distinct lm.user_fk)                                                as contributing_learners
from learner_months lm
join contract_courseruns cc
    on lm.courserun_fk = cc.courserun_pk
-- A row with no month is not a month the API can publish.
where lm.activity_year_and_month is not null
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    lm.activity_year_and_month
