{{ config(
    materialized='table'
) }}

-- User to B2B organization membership bridge.
-- Grain: one row per (user, organization) membership.
with user_organizations as (
    select
        user_id
        , organization_id
        , userorganization_keep_until_seen
        , userorganization_is_manager
    from {{ ref('stg__mitxonline__app__postgres__b2b_userorganization') }}
)

, dim_user as (
    select user_pk, mitxonline_application_user_id
    from {{ ref('dim_user') }}
    where mitxonline_application_user_id is not null
)

, dim_organization as (
    select organization_pk, organization_id
    from {{ ref('dim_organization') }}
    where platform = 'mitxonline'
)

select
    du.user_pk as user_fk
    , orgs.organization_pk as organization_fk
    , uo.userorganization_keep_until_seen
    , uo.userorganization_is_manager
from user_organizations as uo
inner join dim_user as du on uo.user_id = du.mitxonline_application_user_id
inner join dim_organization as orgs on cast(uo.organization_id as varchar) = orgs.organization_id
