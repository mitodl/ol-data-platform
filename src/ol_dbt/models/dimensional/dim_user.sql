{{ config(
    materialized='table'
) }}

with learn_profile as (
    select * from {{ ref('stg__mitlearn__app__postgres__profiles_profile') }}
)

, learn_user_topic_interests as (
    select
        profile_topic_interests.profile_id
        , array_agg(topic.learningresourcetopic_name) as topic_interests
    from {{ ref('stg__mitlearn__app__postgres__profiles_profile_topic_interests') }} as profile_topic_interests
    join {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }} as topic
        on profile_topic_interests.learningresourcetopic_id = topic.learningresourcetopic_id
    group by profile_topic_interests.profile_id

)

-- The per-platform account list this model used to build inline now lives in
-- int__combined__user_accounts, so that int__combined__user_key_map resolves identity from
-- the same accounts rather than rebuilding them.
--
-- Shared email still groups accounts into one person: for MITx Pro, Bootcamps, Residential,
-- Emeritus and Global Alumni it is the only signal we have. This ranking no longer decides
-- the KEY -- int__combined__user_key_map does, once, and never revisits it. It still
-- decides which account's attributes surface below, and the key map applies the identical
-- ranking (same macros) to choose the account a NEW group mints from.
, ranked_accounts as (
    select
        {{ user_account_rank_columns() }}
        , {{ user_account_nk() }} as account_nk
        , accounts.*
    from {{ ref('int__combined__user_accounts') }} as accounts
)

, account_identity as (
    select
        first_value(case
            when user_global_id is not null then 'global'
            when id_source_user_id is not null then id_source
            else 'email'
        end) over w as user_identity_source
        , first_value(coalesce(user_global_id, id_source_user_id, email))
            over w as user_identity_id
        , ranked_accounts.*
    from ranked_accounts
    window w as (
        partition by email
        order by {{ user_account_rank_order() }}
    )
)

-- left join, not inner: an account missing from the map means the map is stale (it can
-- only happen when dim_user is built without it), and a null user_pk fails the not_null
-- test loudly. An inner join would silently drop those users instead.
, account_keys as (
    select
        account_identity.*
        , user_key_map.user_pk as account_user_pk
        , user_key_map.assigned_at
    from account_identity
    left join {{ ref('int__combined__user_key_map') }} as user_key_map
        on account_identity.account_nk = user_key_map.account_nk
)

-- The person's key is the group's SURVIVOR: whichever of its accounts was assigned a key
-- first. Assignment order is immutable, so a group's key cannot move when the ranking
-- shifts underneath it -- which is precisely what used to re-key people mid-build.
, group_survivor as (
    select
        email
        , account_user_pk as user_pk
    from (
        select
            email
            , account_user_pk
            , row_number() over (
                partition by email
                order by assigned_at nulls last, account_user_pk
            ) as survivor_row_num
        from account_keys
    )
    where survivor_row_num = 1
)

, combined_users as (
    select
        group_survivor.user_pk
        , account_keys.*
    from account_keys
    inner join group_survivor on account_keys.email = group_survivor.email
)

-- The base row is the one whose latest join date is newest. Each row nulls the join dates
-- of platforms it isn't from and Trino's greatest() is null-propagating, so every argument
-- is coalesced to '' first - these are ISO8601 strings, so '' sorts below any real date.
, ranked_users as (
    select
        *
        , row_number() over (
            partition by user_pk
            order by
                greatest(
                    coalesce(user_joined_on_mitlearn, '')
                    , coalesce(user_joined_on_mitxonline, '')
                    , coalesce(user_joined_on_edxorg, '')
                    , coalesce(user_joined_on_mitxpro, '')
                    , coalesce(user_joined_on_residential, '')
                    , coalesce(user_joined_on_bootcamps, '')
                ) desc
                , id_source
                , id_source_user_id
        ) as row_num
    from combined_users
)

, base_info as (
    select *
    from ranked_users
    where row_num = 1
)

-- Most recent flexible pricing (financial aid) application per MITxOnline user.
-- Sparse: only populated for users who have submitted an income-based aid application.
, latest_income as (
    select
        user_id
        , flexiblepriceapplication_income_usd as latest_income_usd
        , flexiblepriceapplication_original_income as latest_original_income
        , flexiblepriceapplication_original_currency as latest_original_currency
    from (
        select
            user_id
            , flexiblepriceapplication_income_usd
            , flexiblepriceapplication_original_income
            , flexiblepriceapplication_original_currency
            , row_number() over (
                partition by user_id
                order by flexiblepriceapplication_updated_on desc, flexiblepriceapplication_id desc
            ) as rn
        from {{ ref('int__mitxonline__flexiblepricing_flexiblepriceapplication') }}
    ) as ranked
    where rn = 1
)

, agg_view as (
    select
        user_pk
        , max(user_global_id) as user_global_id
        , max(mitlearn_user_id) as mitlearn_user_id
        , max(mitlearn_openedx_user_id) as mitlearn_openedx_user_id
        , max(mitxonline_openedx_user_id) as mitxonline_openedx_user_id
        , max(mitxonline_application_user_id) as mitxonline_application_user_id
        , max(user_mitxonline_username) as user_mitxonline_username
        , max(mitxpro_openedx_user_id) as mitxpro_openedx_user_id
        , max(mitxpro_application_user_id) as mitxpro_application_user_id
        , max(user_mitxpro_username) as user_mitxpro_username
        , max(residential_openedx_user_id) as residential_openedx_user_id
        , max(user_residential_username) as user_residential_username
        , max(edxorg_openedx_user_id) as edxorg_openedx_user_id
        , max(emeritus_user_id) as emeritus_user_id
        , max(global_alumni_user_id) as global_alumni_user_id
        , max(micromasters_user_id) as micromasters_user_id
        , max(user_edxorg_username) as user_edxorg_username
        , max(user_is_active_on_mitlearn) as user_is_active_on_mitlearn
        , max(user_joined_on_mitlearn) as user_joined_on_mitlearn
        , max(user_is_active_on_mitxonline) as user_is_active_on_mitxonline
        , max(user_joined_on_mitxonline) as user_joined_on_mitxonline
        , max(user_is_active_on_edxorg) as user_is_active_on_edxorg
        , max(user_joined_on_edxorg) as user_joined_on_edxorg
        , max(user_is_active_on_mitxpro) as user_is_active_on_mitxpro
        , max(user_joined_on_mitxpro) as user_joined_on_mitxpro
        , max(user_is_active_on_residential) as user_is_active_on_residential
        , max(user_joined_on_residential) as user_joined_on_residential
        , max(bootcamps_application_user_id) as bootcamps_application_user_id
        , max(user_is_active_on_bootcamps) as user_is_active_on_bootcamps
        , max(user_joined_on_bootcamps) as user_joined_on_bootcamps
        -- Fallback full_name in case the base row (most recent platform) has a null name.
        -- Cross-platform users may have their base row on a platform with null full_name.
        -- FILTER ensures arbitrary() only sees non-null values, making the fallback reliable.
        , arbitrary(full_name) filter (where full_name is not null) as agg_full_name
        -- Fallback address_state for cross-platform users whose base row is from a platform
        -- that null-codes address_state (e.g. Emeritus, Global Alumni, Residential).
        , arbitrary(address_state) filter (where address_state is not null) as agg_address_state
    from combined_users
    group by user_pk
)

select
    base.user_pk
    -- Reported as the platform. The key still hashes the two mitxonline id namespaces
    -- separately, or an application id and an open edX id of equal value would collide.
    , case
        when base.user_identity_source = 'mitxonline_openedx' then 'mitxonline'
        else base.user_identity_source
    end as user_pk_source
    , agg.user_global_id
    , agg.mitlearn_user_id
    , agg.mitlearn_openedx_user_id
    , agg.mitxonline_openedx_user_id
    , agg.mitxonline_application_user_id
    , agg.user_mitxonline_username
    , agg.mitxpro_openedx_user_id
    , agg.mitxpro_application_user_id
    , agg.user_mitxpro_username
    , agg.residential_openedx_user_id
    , agg.user_residential_username
    , agg.edxorg_openedx_user_id
    , agg.user_edxorg_username
    , agg.emeritus_user_id
    , agg.global_alumni_user_id
    , agg.micromasters_user_id
    , base.email
    , coalesce(base.full_name, agg.agg_full_name) as full_name
    , base.address_country
    , base.highest_education
    , base.gender
    , base.birth_year
    , base.company
    , base.job_title
    , base.industry
    , coalesce(base.address_state, agg.agg_address_state) as address_state
    , latest_income.latest_income_usd
    , latest_income.latest_original_income
    , latest_income.latest_original_currency
    , learn_user_topic_interests.topic_interests as topic_interests
    , learn_profile.user_goals as goals
    , learn_profile.user_delivery_preference as delivery_preference
    , learn_profile.user_completed_onboarding as completed_onboarding
    , learn_profile.user_certificate_desired as certificate_desired
    , agg.user_is_active_on_mitlearn
    , agg.user_joined_on_mitlearn
    , agg.user_is_active_on_mitxonline
    , agg.user_joined_on_mitxonline
    , agg.user_is_active_on_edxorg
    , agg.user_joined_on_edxorg
    , agg.user_is_active_on_mitxpro
    , agg.user_joined_on_mitxpro
    , agg.user_is_active_on_residential
    , agg.user_joined_on_residential
    , agg.bootcamps_application_user_id
    , agg.user_is_active_on_bootcamps
    , agg.user_joined_on_bootcamps
from base_info as base
inner join agg_view as agg on base.user_pk = agg.user_pk
left join learn_profile on base.mitlearn_user_id = learn_profile.user_id
left join learn_user_topic_interests on learn_profile.profile_id = learn_user_topic_interests.profile_id
left join latest_income on agg.mitxonline_application_user_id = latest_income.user_id
