with users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, moderators as (
    select user_id
    from {{ ref('stg__mitxonline__app__postgres__users_user_groups') }}
    where group_id = 1
)

, cms_editors as (
    select distinct user_id
    from {{ ref('stg__mitxonline__app__postgres__users_user_groups') }}
    where group_id = 2
)


, users_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

select
    users.id
    , users.username
    , users.full_name
    , users.user_email
    , users.is_open_learning_staff
    , users.user_joined_on_utc
    , users.last_login_utc
    , users_legaladdress.user_address_country
    , users_legaladdress.first_name
    , users_legaladdress.last_name
    , (moderators.user_id is not null) as is_moderator
    , (cms_editors.user_id is not null) as is_cms_editor
from users
left join moderators on moderators.user_id = users.id
left join cms_editors on cms_editors.user_id = users.id
left join users_legaladdress on users_legaladdress.user_id = users.id
