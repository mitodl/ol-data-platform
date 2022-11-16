with users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
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
, users_profiles as (
    select
        micromasters_profiles.user_profile_id as user_micromasters_profile_id
        , micromasters_auth.user_username
        , micromasters_profiles.user_address_country
    from micromasters_profiles
    inner join micromasters_auth on micromasters_auth.user_id = micromasters_profiles.user_id
)

select
    users.user_id
    , users.user_username
    , users.user_full_name
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users_legaladdress.user_address_country
    , users.user_is_active
    , users_profiles.user_micromasters_profile_id
from users
left join users_legaladdress on users_legaladdress.user_id = users.user_id
left join users_profiles on users_profiles.user_username = users.user_username
