with discussions as (
    select * from {{ ref('int__mitxonline__user_courseactivity_discussion') }}
)

, course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    discussions.user_username
    , discussions.courserun_readable_id
    , discussions.useractivity_discussion_page_url as page_url
    , discussions.useractivity_path as discussion_event_path
    , discussions.useractivity_event_type as discussion_event_type
    , discussions.useractivity_timestamp as discussion_event_timestamp
    , discussions.useractivity_discussion_block_id as discussion_id
    , discussions.useractivity_discussion_block_name as discussion_name
    , discussions.useractivity_discussion_post_id as post_id
    , discussions.useractivity_discussion_post_title as post_title
    , users.user_full_name
    , users.user_email
    , course_runs.courserun_title
    , course_runs.course_number
    , course_runs.courserun_start_on
    , course_runs.courserun_end_on
from discussions
inner join course_runs on discussions.courserun_readable_id = course_runs.courserun_readable_id
left join users on discussions.user_username = users.user_username
