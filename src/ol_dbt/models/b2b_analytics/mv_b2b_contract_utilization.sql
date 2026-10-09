{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- learners_certified counts learners enrolled in a contract run who hold its certificate
-- (afact_learner_courserun_progress.is_certified), as mv_b2b_mit_admin_contract_health's
-- certified_learners does.
with enrollments as (
    select
        boc.contract_fk,
        p.user_fk,
        p.enrollment_is_active,
        p.is_certified
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'afact_learner_courserun_progress') }} p
        on boc.courserun_fk = p.courserun_fk
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
    c.b2b_contract_max_learners                                                        as seat_limit,
    c.b2b_contract_membership_type,
    count(distinct e.user_fk)                                                          as seats_consumed,
    count(distinct case when e.enrollment_is_active then e.user_fk end)                as active_learners,
    count(distinct case when e.is_certified then e.user_fk end)                        as learners_certified,
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
