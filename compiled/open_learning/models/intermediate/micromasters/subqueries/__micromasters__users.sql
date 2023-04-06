with users as (
    select * from dev.main_staging.stg__micromasters__app__postgres__auth_user
)

, profiles as (
    select * from dev.main_staging.stg__micromasters__app__postgres__profiles_profile
)

, mitxonline_auth as (
    select
        user_id
        , user_username as user_mitxonline_username
    from dev.main_staging.stg__micromasters__app__postgres__auth_usersocialauth
    where user_auth_provider = 'mitxonline'
)

, edxorg_auth as (
    select
        user_id
        , user_username as user_edxorg_username
    from dev.main_staging.stg__micromasters__app__postgres__auth_usersocialauth
    where user_auth_provider = 'edxorg'
)

, edxorg_users as (
    select
        user_username as user_edxorg_username
        , user_last_login
    from dev.main_intermediate.int__edxorg__mitx_users
)

, most_recent_edx_username as (
    select
        user_id
        , user_edxorg_username
    from (
        select
            edxorg_auth.user_id
            , edxorg_auth.user_edxorg_username
            , row_number() over (
                partition by edxorg_auth.user_id
                order by edxorg_users.user_last_login desc
            ) as row_num
        from edxorg_auth
        left join edxorg_users on edxorg_auth.user_edxorg_username = edxorg_users.user_edxorg_username
    )
    where row_num = 1
)


select
    users.user_id
    , users.user_username
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , profiles.user_profile_id
    , profiles.user_account_privacy
    , profiles.user_full_name
    , profiles.user_first_name
    , profiles.user_last_name
    , profiles.user_address_country
    , profiles.user_address_city
    , profiles.user_address_state_or_territory
    , profiles.user_address_postal_code
    , profiles.user_street_address
    , profiles.user_preferred_name
    , profiles.user_email_is_optin
    , profiles.user_profile_is_filled_out
    , profiles.user_nationality
    , profiles.user_phone_number
    , profiles.user_employer
    , profiles.user_job_title
    , profiles.user_preferred_language
    , profiles.user_mailing_address
    , profiles.user_is_verified
    , profiles.user_has_agreed_to_terms_of_service
    , profiles.user_language_proficiencies
    , profiles.user_profile_parental_consent_is_required
    , profiles.user_bio
    , profiles.user_about_me
    , profiles.user_edx_name
    , profiles.user_edx_goals
    , profiles.user_birth_date
    , profiles.user_gender
    , profiles.user_highest_education
    , mitxonline_auth.user_mitxonline_username
    , most_recent_edx_username.user_edxorg_username
from users
left join profiles on profiles.user_id = users.user_id
left join mitxonline_auth on mitxonline_auth.user_id = users.user_id
left join most_recent_edx_username on most_recent_edx_username.user_id = users.user_id
