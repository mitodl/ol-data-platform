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
-- specific thing, and those cohorts are strict subsets of engaged_learners
-- (active_count is 1 on ANY activity -- see organization_administration_report).
-- ol-analytics-api can only apply its k-anonymity floor to a cohort this view
-- emits, so each such sum publishes its own contributing cohort count
-- (video_watchers, problem_attempters, chatbot_users, certified_learners)
-- alongside it. Do not add an activity aggregate without also emitting the
-- cohort it is attributable to.
with contract_courseruns as (
    select
        cr.courserun_readable_id,
        cr.courserun_title,
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
    cc.courserun_readable_id,
    cc.courserun_title,
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
    sum(oar.certificate_count)                                                  as certificates_earned,
    count(distinct case when oar.certificate_count > 0 then oar.user_email end) as certified_learners
from {{ source('reporting', 'organization_administration_report') }} oar
-- Inner join for the same reason as the contract-scoped trend view: a report
-- row whose course run resolves to no contract has no row to sit under.
join contract_courseruns cc
    on oar.courserun_readable_id = cc.courserun_readable_id
group by
    cc.organization_key,
    cc.sso_organization_id,
    cc.organization_name,
    cc.contract_pk,
    cc.contract_id,
    cc.b2b_contract_name,
    cc.courserun_readable_id,
    cc.courserun_title
