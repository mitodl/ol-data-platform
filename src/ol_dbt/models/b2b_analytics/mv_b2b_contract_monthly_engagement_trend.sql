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
-- A learner is counted under the contract that owns the course run the
-- activity happened in, so a learner active under two contracts contributes to
-- both rows -- the contract rows therefore do not partition the org's learner
-- count, and summing monthly_active_learners across contracts can exceed the
-- org row.
with contract_courseruns as (
    -- courserun -> contract, resolved through the dimensional bridge. A course
    -- run belongs to exactly one B2B contract (courses_courserun.b2b_contract_id),
    -- so this cannot fan out the report rows it is joined to.
    select
        cr.courserun_readable_id,
        c.contract_pk,
        c.contract_id,
        c.b2b_contract_name,
        org.organization_key,
        org.sso_organization_id,
        org.organization_name
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'dim_contract') }} c
        on boc.contract_fk = c.contract_pk
    join {{ source('dimensional', 'dim_organization') }} org
        on c.organization_fk = org.organization_pk
    join {{ source('dimensional', 'dim_course_run') }} cr
        on boc.courserun_fk = cr.courserun_pk
    where org.platform = 'mitxonline'
      and cr.is_current = true
)

select
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
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
-- An inner join, unlike the org-level view's left join to dim_organization:
-- a report row whose course run resolves to no contract has nothing to sit
-- under here. That drops the free-text organization_key fallback rows
-- (coalesce(b2b_contract_to_courseruns.organization_key,
-- user_course_roles.organization) in the source report), which by definition
-- never resolved to a contract-bearing course run.
join contract_courseruns cc
    on oar.courserun_readable_id = cc.courserun_readable_id
where oar.activity_year_and_month is not null
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    oar.activity_year_and_month
