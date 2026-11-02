{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract (all orgs; MIT admin view only). Refreshed by the Dagster
-- b2b_organization MV-refresh asset.
-- health_status: inactive | at_risk | high_utilization | healthy | early_stage
-- Seats and certificates are counted as in mv_b2b_contract_utilization: course enrollments
-- made under the contract, and certificates for those enrollments' runs. Every contract
-- has a row, as there, including one with no enrollments yet.
with contract_stats as (
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
        -- MITx Online documents max_learners 0 and null both as unlimited; null carries
        -- it here, as in mv_b2b_contract_courserun.
        nullif(c.b2b_contract_max_learners, 0)                                          as seat_limit,
        c.b2b_contract_membership_type,
        count(distinct p.user_fk)                                                       as seats_consumed,
        count(distinct case when p.enrollment_is_active then p.user_fk end)             as active_learners,
        count(distinct case when p.is_certified then p.user_fk end)                     as certified_learners
    from {{ source('dimensional', 'dim_contract') }} c
    join {{ source('dimensional', 'dim_organization') }} org
        on c.organization_fk = org.organization_pk
    left join {{ source('dimensional', 'afact_learner_courserun_progress') }} p
        on c.contract_pk = p.contract_fk
    where org.platform = 'mitxonline'
    group by
        org.organization_key, org.sso_organization_id, org.organization_name,
        c.contract_pk, c.contract_id, c.b2b_contract_name, c.b2b_contract_is_active,
        c.b2b_contract_start_date, c.b2b_contract_end_date,
        c.b2b_contract_max_learners, c.b2b_contract_membership_type
)

-- at_risk_since is a threshold date, like needs_attention_since: the reader compares it
-- against the current UTC date, so at_risk does not freeze at this view's last refresh.
-- health_status keeps its current_date() branch until ol-analytics-api reads this.
, contract_thresholds as (
    select
        *,
        case
            when b2b_contract_is_active
                 and round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) < 25
                then cast(date_sub(b2b_contract_end_date, interval 89 day) as date)
        end                                                                     as at_risk_since
    from contract_stats
)

select
    *,
    round(100.0 * seats_consumed / nullif(seat_limit, 0), 1)                   as seat_utilization_pct,
    round(100.0 * certified_learners / nullif(seats_consumed, 0), 1)           as completion_rate_pct,
    case
        when not b2b_contract_is_active                                        then 'inactive'
        when round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) >= 90   then 'high_utilization'
        when at_risk_since <= current_date()                                   then 'at_risk'
        when round(100.0 * seats_consumed / nullif(seat_limit, 0), 1) >= 50   then 'healthy'
        else 'early_stage'
    end                                                                         as health_status
from contract_thresholds
