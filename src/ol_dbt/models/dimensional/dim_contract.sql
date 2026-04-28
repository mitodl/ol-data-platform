{{ config(
    materialized='table'
) }}

-- MITx Online B2B contracts dimension.
-- Grain: one row per contract.
with contracts as (
    select * from {{ ref('stg__mitxonline__app__postgres__b2b_contractpage') }}
)

, dim_organization as (
    select organization_pk, organization_id
    from {{ ref('dim_organization') }}
    where platform = 'mitxonline'
)

select
    {{ dbt_utils.generate_surrogate_key(['b2b_contract_id']) }} as contract_pk
    , b2b_contract_id as contract_id
    , b2b_contract_name
    , b2b_contract_is_active
    , b2b_contract_description
    , b2b_contract_start_date
    , b2b_contract_end_date
    , b2b_contract_max_learners
    , b2b_contract_integration_type
    , b2b_contract_enrollment_fixed_price
    , b2b_contract_membership_type
    , org.organization_pk as organization_fk
from contracts
left join dim_organization as org
    on cast(contracts.organization_id as varchar) = org.organization_id
