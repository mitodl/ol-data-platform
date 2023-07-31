with users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

, users_profile as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_userprofile') }}
)

, micromasters_profiles as (
    select * from {{ ref('stg__micromasters__app__postgres__profiles_profile') }}
)

, micromasters_auth as (
    select *
    from {{ ref('stg__micromasters__app__postgres__auth_usersocialauth') }}
    where user_auth_provider = 'mitxonline'
)

--- this profile is pull from MicroMasters so that we can match MM profile with MITxOnline users based on username
, micromasters_profile as (
    select
        micromasters_profiles.user_profile_id
        , micromasters_profiles.user_id
        , micromasters_auth.user_username
    from micromasters_profiles
    inner join micromasters_auth on micromasters_auth.user_id = micromasters_profiles.user_id
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, openedx_users as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
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
    , users_legaladdress.user_address_state
    , users_legaladdress.user_first_name
    , users_legaladdress.user_last_name
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
    , users_profile.user_type_is_student
    , users_profile.user_type_is_professional
    , users_profile.user_type_is_educator
    , users_profile.user_type_is_other
    , micromasters_profile.user_profile_id as user_micromasters_profile_id
    , micromasters_users.user_edxorg_username
from users
left join users_legaladdress on users_legaladdress.user_id = users.user_id
left join users_profile on users_profile.user_id = users.user_id
left join micromasters_profile on micromasters_profile.user_username = users.user_username
left join micromasters_users
    on micromasters_profile.user_profile_id = micromasters_users.user_profile_id
left join openedx_users
    on users.user_username = openedx_users.user_username
