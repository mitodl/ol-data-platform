with users as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
)

, users_profile as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_profile') }}
)

, openedx_users as (
    select * from {{ ref('stg__mitxpro__openedx__mysql__auth_user') }}
)


select
    users.user_id
    , openedx_users.openedx_user_id
    , users.user_username
    , users.user_full_name
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , users_legaladdress.user_address_country
    , users_legaladdress.user_address_state_or_territory
    , users_legaladdress.user_address_city
    , users_legaladdress.user_street_address
    , users_legaladdress.user_address_postal_code
    , users_legaladdress.user_vat_id
    , users_profile.user_birth_year
    , users_profile.user_company
    , users_profile.user_job_title
    , users_profile.user_industry
    , users_profile.user_job_function
    , users_profile.user_leadership_level
    , users_profile.user_highest_education
    , users_profile.user_gender
    , users_profile.user_company_size
    , users_profile.user_years_experience
from users
left join users_legaladdress on users.user_id = users_legaladdress.user_id
left join users_profile on users.user_id = users_profile.user_id
left join openedx_users
    on users.user_username = openedx_users.user_username
