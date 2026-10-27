{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- A seat is a learner with a course enrollment made under the contract
-- (afact_learner_courserun_progress.contract_fk, carried from tfact_enrollment), and
-- certified_learners counts those learners who hold the run's certificate (is_certified),
-- as mv_b2b_mit_admin_contract_health's certified_learners does. A program enrollment
-- also carries the contract, but takes no seat until the learner enrolls in a course.
-- learners_certified is the old name for certified_learners, kept until ol-analytics-api
-- and mit-learn read the new one.
with enrollments as (
    select
        p.contract_fk,
        p.user_fk,
        p.enrollment_is_active,
        p.is_certified
    from {{ source('dimensional', 'afact_learner_courserun_progress') }} p
    where p.contract_fk is not null
)

select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    c.contract_pk,
    c.contract_id,
    c.b2b_contract_name,
    c.b2b_contract_is_active,
    c.b2b_contract_start_date,
    c.b2b_contract_end_date,
    -- MITx Online documents max_learners 0 and null both as unlimited; null carries it here,
    -- as in mv_b2b_contract_courserun.
    nullif(c.b2b_contract_max_learners, 0)                                             as seat_limit,
    c.b2b_contract_membership_type,
    count(distinct e.user_fk)                                                          as seats_consumed,
    count(distinct case when e.enrollment_is_active then e.user_fk end)                as active_learners,
    count(distinct case when e.is_certified then e.user_fk end)                        as learners_certified,
    count(distinct case when e.is_certified then e.user_fk end)                        as certified_learners,
    round(100.0 * count(distinct e.user_fk)
        / nullif(c.b2b_contract_max_learners, 0), 1)                                   as seat_utilization_pct,
    round(100.0 * count(distinct case when e.is_certified then e.user_fk end)
        / nullif(count(distinct e.user_fk), 0), 1)                                    as completion_rate_pct
from {{ source('dimensional', 'dim_contract') }} c
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
left join enrollments e on c.contract_pk = e.contract_fk
where org.platform = 'mitxonline'
group by
    org.organization_key, org.sso_organization_id, org.organization_name,
    c.contract_pk, c.contract_id, c.b2b_contract_name, c.b2b_contract_is_active,
    c.b2b_contract_start_date, c.b2b_contract_end_date,
    c.b2b_contract_max_learners, c.b2b_contract_membership_type
