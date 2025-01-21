with users as (
    select * from {{ ref('__micromasters__users') }}
)

, education as (
    select
        user_profile_id
        , user_education_degree
        , user_graduation_date
        , row_number() over (partition by user_profile_id order by user_graduation_date desc) as row_num
    from {{ ref('stg__micromasters__app__postgres__profiles_education') }}
)

, highest_education as (
    select
        user_profile_id
        , user_education_degree
    from education
    where row_num = 1
)

, employment as (
    select
        user_profile_id
        , user_company_name
        , user_job_position
        , user_company_industry
        , row_number() over (partition by user_profile_id order by user_start_date desc) as row_num
    from {{ ref('stg__micromasters__app__postgres__profiles_employment') }}
)

, most_recent_employment as (
    select
        user_profile_id
        , user_company_name
        , user_job_position
        , user_company_industry
    from employment
    where row_num = 1
)

select
    users.user_id
    , users.user_username
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , users.user_profile_id
    , users.user_full_name
    , users.user_first_name
    , users.user_last_name
    , users.user_birth_country
    , users.user_address_country
    , users.user_address_city
    , users.user_address_state_or_territory
    , users.user_address_postal_code
    , users.user_street_address
    , users.user_mailing_address
    , users.user_preferred_name
    , users.user_preferred_language
    , users.user_nationality
    , users.user_phone_number
    , users.user_bio
    , users.user_about_me
    , users.user_edx_name
    , users.user_edx_goals
    , users.user_birth_date
    , users.user_gender
    , users.user_mitxonline_username
    , users.user_edxorg_username
    , most_recent_employment.user_company_name
    , most_recent_employment.user_company_industry
    , most_recent_employment.user_job_position
    , coalesce(highest_education.user_education_degree, users.user_highest_education) as user_highest_education
from users
left join highest_education on users.user_profile_id = highest_education.user_profile_id
left join most_recent_employment on users.user_profile_id = most_recent_employment.user_profile_id
