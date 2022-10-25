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
    , users_legaladdress.user_address_country
from users
left join users_legaladdress on users_legaladdress.user_id = users.user_id
left join users_profile on users_profile.user_id = users.user_id
where users.user_is_active = true
