with users as (
    select
        *
        , {{ element_at_array('split(user_full_name, \' \')', 1) }} as user_first_name
        , if(
            {{ array_length("split(user_full_name, ' ')") }} > 1
           , {{ element_at_array('split(user_full_name, \' \')', -1) }}
           , null
        ) as user_last_name
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
    inner join micromasters_auth on micromasters_profiles.user_id = micromasters_auth.user_id
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, openedx_users as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
)

-- A user can hold more than one Open edX username (e.g. one renamed when the account was
-- retired). Keep one, or every join on user_id below repeats the user, and so does every
-- model that reads this one: synced first, then the latest.
, openedxuser_mapping as (
    select *
    from (
        select
            *
            , row_number() over (
                partition by user_id
                order by
                    openedxuser_has_been_synced desc nulls last
                    , openedxuser_updated_on desc nulls last
                    , openedxuser_id desc
            ) as openedxuser_row_num
        from {{ ref('stg__mitxonline__app__postgres__openedx_openedxuser') }}
    ) as ranked
    where openedxuser_row_num = 1
)

select
    users.user_id
    , users.user_global_id
    , openedx_users.openedx_user_id
    , users.user_full_name
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , users_legaladdress.user_address_country
    , users_legaladdress.user_address_state
    , users.user_first_name
    , users.user_last_name
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
    , coalesce(openedx_users.user_username, users.user_username) as user_username
from users
left join users_legaladdress on users.user_id = users_legaladdress.user_id
left join users_profile on users.user_id = users_profile.user_id
left join openedxuser_mapping
    on users.user_id = openedxuser_mapping.user_id
left join openedx_users
    on openedxuser_mapping.openedxuser_username = openedx_users.user_username
left join micromasters_profile
    on openedxuser_mapping.openedxuser_username = micromasters_profile.user_username
left join micromasters_users
    on micromasters_profile.user_profile_id = micromasters_users.user_profile_id
