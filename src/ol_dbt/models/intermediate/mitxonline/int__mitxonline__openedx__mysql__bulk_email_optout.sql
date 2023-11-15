-- Email optout information for MITx Online openededx

with email_optout as (
    select *
    from {{ ref('stg__mitxonline__openedx__mysql__bulk_email_optout') }}
)

, courserun as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, users as (
    select *
    from {{ ref('int__mitxonline__users') }}
)

select
    email_optout.email_optout_id
    , email_optout.openedx_user_id
    , email_optout.courserun_readable_id
    , courserun.courserun_title
    , users.user_full_name
    , users.user_username
    , users.user_email
from email_optout
inner join users
    on email_optout.openedx_user_id = users.openedx_user_id
left join courserun
    on email_optout.courserun_readable_id = courserun.courserun_readable_id
