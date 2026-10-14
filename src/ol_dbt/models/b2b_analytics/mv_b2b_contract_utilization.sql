{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract. Refreshed by the Dagster b2b_organization MV-refresh asset.
-- A seat is a learner with a course enrollment made under the contract
-- (tfact_enrollment.contract_fk), and a certificate counts when it is for one of those
-- enrollments' runs. A program enrollment also carries the contract, but takes no seat
-- until the learner enrolls in a course.
with enrollments as (
    select
        e.contract_fk,
        e.user_fk,
        e.courserun_fk,
        e.enrollment_is_active
    from {{ source('dimensional', 'tfact_enrollment') }} e
    where e.contract_fk is not null
      and e.enrollment_type = 'course'
),

certificates as (
    select
        e.contract_fk,
        cert.user_fk
    from enrollments e
    join {{ source('dimensional', 'tfact_certificate') }} cert
        on e.user_fk = cert.user_fk and e.courserun_fk = cert.courserun_fk
    where cert.certificate_is_revoked = false
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
    org.organization_key, org.sso_organization_id, org.organization_name,
    c.contract_pk, c.contract_id, c.b2b_contract_name, c.b2b_contract_is_active,
    c.b2b_contract_start_date, c.b2b_contract_end_date,
    c.b2b_contract_max_learners, c.b2b_contract_membership_type
