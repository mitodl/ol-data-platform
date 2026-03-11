{{ config(
    materialized='table'
) }}

-- Maps MITx Online B2B organizations to course runs via B2B contracts.
-- Grain: one row per (organization, course_run, contract).
-- Note: xPro B2B purchases are tracked via orders (see int__mitxpro__b2becommerce_b2border).
with b2b_mappings as (
    select
        courserun_readable_id
        , organization_key
        , organization_name
        , b2b_contract_name
        , b2b_contract_is_active
        , b2b_contract_start_date
        , b2b_contract_end_date
    from {{ ref('int__mitxonline__b2b_contract_to_courseruns') }}
)

, dim_organization as (
    select organization_pk, organization_key, platform
    from {{ ref('dim_organization') }}
    where platform = 'mitxonline'
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where platform = 'mitxonline'
      and is_current = true
)

select
    org.organization_pk as organization_fk
    , cr.courserun_pk as courserun_fk
    , b2b_mappings.b2b_contract_name
    , b2b_mappings.b2b_contract_is_active
    , b2b_mappings.b2b_contract_start_date
    , b2b_mappings.b2b_contract_end_date
from b2b_mappings
inner join dim_organization as org
    on b2b_mappings.organization_key = org.organization_key
inner join dim_course_run as cr
    on b2b_mappings.courserun_readable_id = cr.courserun_readable_id
