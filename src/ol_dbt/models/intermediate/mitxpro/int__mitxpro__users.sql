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

, company as (
    select * from {{ ref('stg__mitxpro__app__postgres__ecommerce_company') }}
)

, user_company_from_enrollments as (
    select
        user_id
        , ecommerce_company_id
        , row_number() over (partition by user_id order by courserunenrollment_created_on desc) as enrollment_rank
    from {{ ref('stg__mitxpro__app__postgres__courses_courserunenrollment') }}
    where ecommerce_company_id is not null
)

, user_company as (
    select
        user_company_from_enrollments.user_id
        , company.company_name
    from user_company_from_enrollments
    inner join company on user_company_from_enrollments.ecommerce_company_id = company.company_id
    where user_company_from_enrollments.enrollment_rank = 1
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
    , users_profile.user_job_title
    , users_profile.user_industry
    , users_profile.user_job_function
    , users_profile.user_leadership_level
    , users_profile.user_highest_education
    , users_profile.user_gender
    , users_profile.user_company_size
    , users_profile.user_years_experience
    , coalesce(user_company.company_name, users_profile.user_company) as user_company
from users
left join users_legaladdress on users.user_id = users_legaladdress.user_id
left join users_profile on users.user_id = users_profile.user_id
left join openedx_users
    on users.user_username = openedx_users.user_username
left join user_company on users.user_id = user_company.user_id
