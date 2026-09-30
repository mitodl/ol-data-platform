{{ config(
    materialized='table'
) }}

-- User to B2B contract membership bridge, carrying the learner's data-sharing consent.
-- Grain: one row per (user, contract).
-- Consent is recorded per contract, not per organization, so a learner seated under
-- two of an organization's contracts can share under one and not the other.
-- MITx Online hard-deletes a membership when the learner leaves the organization, and
-- a re-added learner starts with no decision, while their enrollments (and so their
-- outcomes) stay in the learner-records views. The decision is therefore kept from
-- the snapshot: when the current membership has no decision, or is gone, the row
-- carries the latest decision ever recorded for the (user, contract), consent or
-- decline.
with current_memberships as (
    select
        user_id
        , contract_id
        , userb2bcontract_consented_to_data_sharing
        , userb2bcontract_consent_modified_at
        , userb2bcontract_created_on
        , userb2bcontract_updated_on
    from {{ ref('stg__mitxonline__app__postgres__b2b_userb2bcontract') }}
)

, decision_history as (
    select
        user_id
        , contract_id
        , userb2bcontract_consented_to_data_sharing
        , userb2bcontract_consent_modified_at
        , row_number() over (
            partition by user_id, contract_id
            order by userb2bcontract_consent_modified_at desc, dbt_valid_from desc
        ) as decision_rank
    from {{ ref('snapshot_mitxonline_b2b_userb2bcontract') }}
    where userb2bcontract_consented_to_data_sharing is not null
)

, last_decisions as (
    select
        user_id
        , contract_id
        , userb2bcontract_consented_to_data_sharing
        , userb2bcontract_consent_modified_at
    from decision_history
    where decision_rank = 1
)

, resolved as (
    select
        coalesce(cm.user_id, ld.user_id) as user_id
        , coalesce(cm.contract_id, ld.contract_id) as contract_id
        , cm.user_id is not null as membership_is_current
        , case
            when cm.userb2bcontract_consented_to_data_sharing is not null
                then cm.userb2bcontract_consented_to_data_sharing
            else ld.userb2bcontract_consented_to_data_sharing
        end as consented_to_data_sharing
        , case
            when cm.userb2bcontract_consented_to_data_sharing is not null
                then cm.userb2bcontract_consent_modified_at
            else ld.userb2bcontract_consent_modified_at
        end as consent_modified_at
        , cm.userb2bcontract_created_on as membership_created_on
        , cm.userb2bcontract_updated_on as membership_updated_on
    from current_memberships as cm
    full outer join last_decisions as ld
        on cm.user_id = ld.user_id and cm.contract_id = ld.contract_id
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
    , r.membership_is_current
    , r.consented_to_data_sharing
    , r.consent_modified_at
    , r.membership_created_on
    , r.membership_updated_on
from resolved as r
inner join dim_user as du on r.user_id = du.mitxonline_application_user_id
inner join dim_contract as dc on r.contract_id = dc.contract_id
