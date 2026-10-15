{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- Learners are attributed to the contract they enrolled under
-- (afact_learner_courserun_progress.contract_fk), not to every contract that offers the run.
select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    c.contract_pk,
    c.contract_id,
    c.b2b_contract_name,
    cr.courserun_pk,
    cr.courserun_readable_id,
    cr.courserun_title,
    count(distinct p.user_fk)                                                           as enrolled_learners,
    count(distinct case when p.enrollment_is_active then p.user_fk end)                 as active_learners,
    count(distinct case when p.is_passing then p.user_fk end)                           as passing_learners,
    count(distinct case when p.is_certified then p.user_fk end)                         as certified_learners,
    round(100.0 * count(distinct case when p.enrollment_is_active then p.user_fk end)
        / nullif(count(distinct p.user_fk), 0), 1)                                     as active_rate_pct,
    round(100.0 * count(distinct case when p.is_certified then p.user_fk end)
        / nullif(count(distinct p.user_fk), 0), 1)                                     as completion_rate_pct
from {{ source('dimensional', 'afact_learner_courserun_progress') }} p
join {{ source('dimensional', 'dim_contract') }} c
    on p.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_course_run') }} cr
    on p.courserun_fk = cr.courserun_pk
where org.platform = 'mitxonline'
  and cr.is_current = true
group by
    org.organization_key, org.sso_organization_id, org.organization_name,
    c.contract_pk, c.contract_id, c.b2b_contract_name,
    cr.courserun_pk, cr.courserun_readable_id, cr.courserun_title
