-- MITxOnline open edx users
-- As of 03/2023, the available user-related fields from user_info_combo are email, username, last_login,
-- date_joined and is_staff

with mitxonline_openedx_users as (
    select * from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where courserun_platform = '{{ var("mitxonline") }}'
)

, mitxonline_app_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, mitxonline_openedx_users_with_row_number as (
    select
        user_id
        , user_email
        , user_username
        , user_last_login
        , user_joined_on
        , row_number() over (partition by user_id order by user_last_login desc) as row_num
    from mitxonline_openedx_users
)

, most_recent_mitxonline_openedx_users as (
    select
        user_id
        , user_email
        , user_username
        , user_last_login
        , user_joined_on
    from mitxonline_openedx_users_with_row_number
    where row_num = 1
)

select
    most_recent_mitxonline_openedx_users.user_id
    , most_recent_mitxonline_openedx_users.user_email
    , most_recent_mitxonline_openedx_users.user_username
    , most_recent_mitxonline_openedx_users.user_last_login
    , most_recent_mitxonline_openedx_users.user_joined_on
    , mitxonline_app_users.user_id as mitxonline_user_id
from most_recent_mitxonline_openedx_users
inner join mitxonline_app_users
    on most_recent_mitxonline_openedx_users.user_email = mitxonline_app_users.user_email
