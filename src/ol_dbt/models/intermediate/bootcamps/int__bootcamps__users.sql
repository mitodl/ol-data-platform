with users as (
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

select
    users.user_id
    , users.user_username
    , users_profile.user_full_name
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , users_legaladdress.user_address_country
    , users_legaladdress.user_address_state_or_territory
    , users_legaladdress.user_address_city
    , users_legaladdress.user_street_address
    , users_legaladdress.user_address_postal_code
    , users_profile.user_birth_year
    , users_profile.user_company
    , users_profile.user_job_title
    , users_profile.user_industry
    , users_profile.user_job_function
    , users_profile.user_highest_education
    , users_profile.user_gender
    , users_profile.user_company_size
    , users_profile.user_years_experience
from users
left join users_legaladdress on users.user_id = users_legaladdress.user_id
left join users_profile on users.user_id = users_profile.user_id
