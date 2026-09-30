{{ config(
    materialized='table'
) }}

-- User to B2B contract membership bridge, carrying the learner's data-sharing consent.
-- Grain: one row per (user, contract) membership.
-- Consent is recorded per contract, not per organization, so a learner seated under
-- two of an organization's contracts can share under one and not the other.
with user_contracts as (
    select
        user_id
        , contract_id
        , userb2bcontract_consented_to_data_sharing
        , userb2bcontract_consent_modified_at
        , userb2bcontract_created_on
        , userb2bcontract_updated_on
    from {{ ref('stg__mitxonline__app__postgres__b2b_userb2bcontract') }}
)

, dim_user as (
    select user_pk, mitxonline_application_user_id
    from {{ ref('dim_user') }}
    where mitxonline_application_user_id is not null
)

, dim_contract as (
    select contract_pk, contract_id, organization_fk
    from {{ ref('dim_contract') }}
)

select
    du.user_pk as user_fk
    , dc.contract_pk as contract_fk
    , dc.organization_fk
    , uc.userb2bcontract_consented_to_data_sharing as consented_to_data_sharing
    , uc.userb2bcontract_consent_modified_at as consent_modified_at
    , uc.userb2bcontract_created_on as membership_created_on
    , uc.userb2bcontract_updated_on as membership_updated_on
from user_contracts as uc
inner join dim_user as du on uc.user_id = du.mitxonline_application_user_id
inner join dim_contract as dc on uc.contract_id = dc.contract_id
