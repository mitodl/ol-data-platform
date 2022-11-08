with users as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
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
from users
left join users_legaladdress on users_legaladdress.user_id = users.user_id
