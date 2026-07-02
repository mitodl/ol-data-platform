{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract (all orgs; MIT admin view only). Refreshed by the Dagster
-- b2b_organization MV-refresh asset.
-- health_status: inactive | at_risk | high_utilization | healthy | early_stage
with contract_stats as (
    select
        org.organization_key,
        org.organization_name,
        c.contract_pk,
        c.b2b_contract_name,
        c.b2b_contract_is_active,
        c.b2b_contract_start_date,
        c.b2b_contract_end_date,
        c.b2b_contract_max_learners                                                     as seat_limit,
        c.b2b_contract_membership_type,
        count(distinct e.user_fk)                                                       as seats_consumed,
        count(distinct case when e.enrollment_is_active then e.user_fk end)             as active_learners,
        count(distinct case when cert.certificate_is_revoked = false
            then cert.user_fk end)                                                      as certified_learners
    from {{ source('dimensional', 'dim_contract') }} c
    join {{ source('dimensional', 'dim_organization') }} org
        on c.organization_fk = org.organization_pk
    join {{ source('dimensional', 'bridge_organization_courserun') }} boc
        on c.contract_pk = boc.contract_fk
    left join {{ source('dimensional', 'tfact_enrollment') }} e
        on boc.courserun_fk = e.courserun_fk
    left join {{ source('dimensional', 'tfact_certificate') }} cert
        on e.user_fk = cert.user_fk
        and boc.courserun_fk = cert.courserun_fk
        and cert.certificate_is_revoked = false
    where org.platform = 'mitxonline'
    group by
        org.organization_key, org.organization_name,
        c.contract_pk, c.b2b_contract_name, c.b2b_contract_is_active,
        c.b2b_contract_start_date, c.b2b_contract_end_date,
        c.b2b_contract_max_learners, c.b2b_contract_membership_type
)

select
    *,
    round(100.0 * seats_consumed / nullif(seat_limit, 0), 1)                   as seat_utilization_pct,
    round(100.0 * certified_learners / nullif(seats_consumed, 0), 1)           as completion_rate_pct,
    case
        when not b2b_contract_is_active                                        then 'inactive'
        when round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) >= 90   then 'high_utilization'
        when round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) < 25
             and datediff(b2b_contract_end_date, current_date()) < 90          then 'at_risk'
        when round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) >= 50   then 'healthy'
        else 'early_stage'
    end                                                                         as health_status
from contract_stats
