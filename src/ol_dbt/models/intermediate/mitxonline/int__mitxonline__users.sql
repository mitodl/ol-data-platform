with users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

select
    users.id
    , users.username
    , users.full_name
    , users.email
    , users.joined_on
    , users.last_login
    , users_legaladdress.user_address_country
    , users_legaladdress.first_name
    , users_legaladdress.last_name
from users
left join users_legaladdress on users_legaladdress.user_id = users.id
where users.is_active = true
