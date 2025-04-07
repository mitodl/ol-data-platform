with mitxonline_users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

, mitxonline_profile as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_userprofile') }}
)

, mitxonline_user_view as (
    select
        mitxonline_users.user_username
        , mitxonline_users.user_email
        , mitxonline_users.user_full_name
        , mitxonline_legaladdress.user_address_country
        , mitxonline_profile.user_highest_education
        , mitxonline_profile.user_gender
        , mitxonline_profile.user_birth_year
        , mitxonline_profile.user_company
        , mitxonline_profile.user_job_title
        , mitxonline_profile.user_industry
        , mitxonline_users.user_is_active
        , 'mitxonline' as platform
        , mitxonline_users.user_id
        , mitxonline_users.user_joined_on
    from mitxonline_users
    left join mitxonline_legaladdress on mitxonline_users.user_id = mitxonline_legaladdress.user_id
    left join mitxonline_profile on mitxonline_users.user_id = mitxonline_profile.user_id
)

select
{{ dbt_utils.generate_surrogate_key(['user_email']) }} as user_pk
    , user_id as user_mitxonline_id
    , user_username as user_mitxonline_username
    , null as user_edxorg_username
    , null as user_mitxpro_username
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
from mitxonline_user_view
