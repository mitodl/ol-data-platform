with mitx_users_pull as (
    select *
    from {{ ref('int__mitx__users') }}
)

, mitx_users as (
    select
        mitx_users_pull.*
        , "Micromasters" as platform
    from mitx_users_pull
)

, xpro_users as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, xpro_users_profile as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_profile') }}
)

, xpro_users_legaladdress as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
)

, xpro_user_view as (
    select
        xpro_users.user_username
        , xpro_users.user_email
        , xpro_users.user_full_name
        , xpro_users_legaladdress.user_address_country
        , xpro_users_profile.user_highest_education
        , xpro_users_profile.user_gender
        , xpro_users_profile.user_birth_year
        , xpro_users_profile.user_company
        , xpro_users_profile.user_job_title
        , xpro_users_profile.user_industry
        , xpro_users.user_is_active
        , 'mitxpro' as platform
        , xpro_users.user_id
        , xpro_users.user_joined_on
    from xpro_users
    left join xpro_users_profile on xpro_users.user_id = xpro_users_profile.user_id
    left join xpro_users_legaladdress on xpro_users.user_id = xpro_users_legaladdress.user_id
)

, users as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__profiles_legaladdress') }}
)

, users_profile as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__profiles_profile') }}
)

, bootcamps_user_view as (
    select
        users.user_username
        , users.user_email
        , users_profile.user_full_name
        , users_legaladdress.user_address_country
        , users_profile.user_highest_education
        , users_profile.user_gender
        , users_profile.user_birth_year
        , users_profile.user_company
        , users_profile.user_job_title
        , users_profile.user_industry
        , users.user_is_active
        , 'bootcamps' as platform
        , users.user_id
        , users.user_joined_on
    from users
    left join users_legaladdress on users.user_id = users_legaladdress.user_id
    left join users_profile on users.user_id = users_profile.user_id
)

, mitxresidential_users as (
    select *
    from {{ ref('stg__mitxresidential__openedx__auth_user') }}
)

, mitxresidential_profiles as (
    select *
    from {{ ref('stg__mitxresidential__openedx__auth_userprofile') }}
)

, mitxresidential_user_view as (
    select
        mitxresidential_users.user_username
        , mitxresidential_users.user_email
        , mitxresidential_profiles.user_address_country
        , mitxresidential_profiles.user_highest_education
        , mitxresidential_profiles.user_gender
        , mitxresidential_profiles.user_birth_year
        , mitxresidential_users.user_is_active
        , 'residential' as platform
        , mitxresidential_users.user_id
        , mitxresidential_users.user_joined_on
        , coalesce(mitxresidential_users.user_full_name, mitxresidential_profiles.user_full_name)
        as user_full_name
    from mitxresidential_users
    left join mitxresidential_profiles on mitxresidential_users.user_id = mitxresidential_profiles.user_id
)

select
    coalesce(
        user_hashed_id
        , {{ generate_hash_id('cast(user_micromasters_id as varchar) || platform') }} )
    as user_id
    , user_mitxonline_username
    , user_edxorg_username
    , null as user_mitxpro_username
    , null as user_bootcamps_username
    , null as user_residential_username
    , coalesce(
        user_mitxonline_email, user_edxorg_email, user_micromasters_email
    ) as email
    , user_full_name as full_name
    , user_address_country as address_country
    , user_highest_education as highest_education
    , user_gender as gender
    , user_birth_year as birth_year
    , user_company as company
    , user_job_title as job_title
    , user_industry as industry
    , case
        when
            user_is_active_on_mitxonline = true
            or user_is_active_on_edxorg = true then true
    end as user_is_active
    , case
        when user_joined_on_mitxonline >= user_joined_on_edxorg
            then user_joined_on_edxorg
        when user_joined_on_mitxonline is null
            then user_joined_on_edxorg
        else user_joined_on_mitxonline
    end as user_joined_on
from mitx_users

union all

select
    {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , null as user_mitxonline_username
    , null as user_edxorg_username
    , user_username as user_mitxpro_username
    , null as user_bootcamps_username
    , null as user_residential_username
    , user_email as email
    , user_full_name as full_name
    , user_address_country as address_country
    , user_highest_education as highest_education
    , user_gender as gender
    , user_birth_year as birth_year
    , user_company as company
    , user_job_title as job_title
    , user_industry as industry
    , user_is_active
    , user_joined_on
from xpro_user_view

union all

select
    {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , null as user_mitxonline_username
    , null as user_edxorg_username
    , null as user_mitxpro_username
    , user_username as user_bootcamps_username
    , null as user_residential_username
    , user_email as email
    , user_full_name as full_name
    , user_address_country as address_country
    , user_highest_education as highest_education
    , user_gender as gender
    , user_birth_year as birth_year
    , user_company as company
    , user_job_title as job_title
    , user_industry as industry
    , user_is_active
    , user_joined_on
from bootcamps_user_view

union all

select
    {{ generate_hash_id('cast(user_id as varchar) || platform') }} as user_id
    , null as user_mitxonline_username
    , null as user_edxorg_username
    , null as user_mitxpro_username
    , null as user_bootcamps_username
    , user_username as user_residential_username
    , user_email as email
    , user_full_name as full_name
    , user_address_country as address_country
    , user_highest_education as highest_education
    , user_gender as gender
    , user_birth_year as birth_year
    , null as company
    , null as job_title
    , null as industry
    , user_is_active
    , user_joined_on
from mitxresidential_user_view
