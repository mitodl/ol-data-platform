{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract. Refreshed by the Dagster b2b_organization MV-refresh asset.
with enrollments as (
    select
        boc.contract_fk,
        e.user_fk,
        e.enrollment_is_active
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'tfact_enrollment') }} e
        on boc.courserun_fk = e.courserun_fk
),

certificates as (
    select
        boc.contract_fk,
        cert.user_fk
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'tfact_certificate') }} cert
        on boc.courserun_fk = cert.courserun_fk
    where cert.certificate_is_revoked = false
)

select
    org.organization_key,
    org.organization_name,
    c.contract_pk,
    c.b2b_contract_name,
    c.b2b_contract_is_active,
    c.b2b_contract_start_date,
    c.b2b_contract_end_date,
    c.b2b_contract_max_learners                                                        as seat_limit,
    c.b2b_contract_membership_type,
    count(distinct e.user_fk)                                                          as seats_consumed,
    count(distinct case when e.enrollment_is_active then e.user_fk end)                as active_learners,
    count(distinct cert.user_fk)                                                       as learners_certified,
    round(100.0 * count(distinct e.user_fk)
        / nullif(c.b2b_contract_max_learners, 0), 1)                                   as seat_utilization_pct,
    round(100.0 * count(distinct cert.user_fk)
        / nullif(count(distinct e.user_fk), 0), 1)                                    as completion_rate_pct
from {{ source('dimensional', 'dim_contract') }} c
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
left join enrollments e on c.contract_pk = e.contract_fk
left join certificates cert on c.contract_pk = cert.contract_fk
where org.platform = 'mitxonline'
group by
    org.organization_key, org.organization_name,
    c.contract_pk, c.b2b_contract_name, c.b2b_contract_is_active,
    c.b2b_contract_start_date, c.b2b_contract_end_date,
    c.b2b_contract_max_learners, c.b2b_contract_membership_type
