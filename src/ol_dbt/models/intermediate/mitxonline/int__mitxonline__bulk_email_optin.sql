-- Email optin information for MITx Online openededx
with
    email_optout as (select * from {{ ref("stg__mitxonline__openedx__mysql__bulk_email_optout") }}),
    courserun as (select * from {{ ref("stg__mitxonline__app__postgres__courses_courserun") }}),
    users as (select * from {{ ref("int__mitxonline__users") }})

select
    users.openedx_user_id,
    users.user_full_name,
    users.user_username,
    users.user_email,
    email_optout.courserun_readable_id,
    courserun.courserun_title,
    case when email_optout.email_optout_id is null then 1 else 0 end as email_opted_in
from users
left join email_optout on users.openedx_user_id = email_optout.openedx_user_id
left join courserun on email_optout.courserun_readable_id = courserun.courserun_readable_id
where users.openedx_user_id is not null
