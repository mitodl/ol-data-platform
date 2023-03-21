with users as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_user') }}
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
from users
