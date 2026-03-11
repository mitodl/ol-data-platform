{{ config(
    materialized='table'
) }}

-- B2B organizations: MITx Online enterprise organizations and xPro corporate clients.
-- Grain: one row per unique organization per platform.
with mitxonline_organizations as (
    select
        cast(organization_id as varchar) as source_id
        , organization_name
        , organization_key
        , organization_logo
        , organization_description
        , cast(sso_organization_id as varchar) as sso_organization_id
        , 'mitxonline' as platform
    from {{ ref('stg__mitxonline__app__postgres__b2b_organizationpage') }}
)

, mitxpro_companies as (
    select
        cast(company_id as varchar) as source_id
        , company_name as organization_name
        , cast(null as varchar) as organization_key
        , cast(null as varchar) as organization_logo
        , cast(null as varchar) as organization_description
        , cast(null as varchar) as sso_organization_id
        , 'mitxpro' as platform
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_company') }}
)

, combined as (
    select * from mitxonline_organizations
    union all
    select * from mitxpro_companies
)

select
    {{ dbt_utils.generate_surrogate_key(['platform', 'source_id']) }} as organization_pk
    , source_id as organization_id
    , organization_name
    , organization_key
    , organization_logo
    , organization_description
    , sso_organization_id
    , platform
from combined
