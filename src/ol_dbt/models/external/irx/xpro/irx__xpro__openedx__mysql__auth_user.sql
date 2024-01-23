with auth_user as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__auth_user') }}
)

, student_courseenrollment as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__student_courseenrollment') }}
)

select
    auth_user.id
    , auth_user.username
    , auth_user.first_name
    , auth_user.last_name
    , auth_user.email
    , '' as pass_word
    , auth_user.is_staff
    , auth_user.is_active
    , auth_user.is_superuser
    , auth_user.last_login
    , auth_user.date_joined
    , '' as status
    , NULL as email_key
    , '' as avatar_type
    , '' as country
    , 0 as show_country
    , NULL as date_of_birth
    , '' as interesting_tags
    , '' as ignored_tags
    , 0 as email_tag_filter_strategy
    , 0 as display_tag_filter_strategy
    , 0 as consecutive_days_visit_count
from auth_user
inner join student_courseenrollment on auth_user.id = student_courseenrollment.user_id
