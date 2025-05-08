with mitxonline_users as (
    select
        user_id
        , user_username
        , user_email
        , user_full_name
        , user_is_active
        , user_joined_on
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_legaladdress as (
    select
        user_id
        , user_address_country
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

, mitxonline_profile as (
    select
        user_id
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from {{ ref('stg__mitxonline__app__postgres__users_userprofile') }}
)

, openedx_users as (
    select
        openedx_user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
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
    {{ dbt_utils.generate_surrogate_key(['mitxonline_user_view.user_email']) }} as user_pk
    , openedx_users.openedx_user_id as mitxonline_openedx_user_id
    , mitxonline_user_view.user_id as mitxonline_application_user_id
    , mitxonline_user_view.user_username as user_mitxonline_username
    , cast(null as bigint) as mitxpro_openedx_user_id
    , cast(null as bigint) as mitxpro_application_user_id
    , cast(null as varchar) as user_mitxpro_username
    , cast(null as bigint) as residential_openedx_user_id
    , cast(null as varchar) as user_residential_username
    , cast(null as bigint) as edxorg_openedx_user_id
    , cast(null as varchar) as user_edxorg_username
    , mitxonline_user_view.user_email as email
    , mitxonline_user_view.user_full_name as full_name
    , mitxonline_user_view.user_address_country as address_country
    , mitxonline_user_view.user_highest_education as highest_education
    , mitxonline_user_view.user_gender as gender
    , mitxonline_user_view.user_birth_year as birth_year
    , mitxonline_user_view.user_company as company
    , mitxonline_user_view.user_job_title as job_title
    , mitxonline_user_view.user_industry as industry
    , mitxonline_user_view.user_is_active
    , mitxonline_user_view.user_joined_on
from mitxonline_user_view
left join openedx_users
    on (
        mitxonline_user_view.user_username = openedx_users.user_username
        or mitxonline_user_view.user_email = openedx_users.user_email
    )
