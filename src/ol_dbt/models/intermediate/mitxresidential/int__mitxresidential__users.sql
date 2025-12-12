with users as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_user') }}
)

, profiles as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_userprofile') }}
)

select
    users.user_id
    , users.user_username
    , users.user_first_name
    , users.user_last_name
    , users.user_email
    , users.user_joined_on
    , users.user_last_login
    , users.user_is_active
    , profiles.user_address_city
    , profiles.user_address_country
    , profiles.user_birth_year
    , profiles.user_gender
    , profiles.user_highest_education
    , coalesce(users.user_full_name, profiles.user_full_name) as user_full_name
    , cast(null as varchar) as user_street_address
    , cast(null as varchar) as user_address_state_or_territory
    , cast(null as varchar) as user_address_postal_code
from users
left join profiles on users.user_id = profiles.user_id
