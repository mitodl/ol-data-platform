{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select
    user_username
    , courserun_readable_id
    , openedx_user_id
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_path
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.id'") }} as useractivity_discussion_post_id
    , {{ json_query_string('useractivity_event_object', "'$.title'") }} as useractivity_discussion_post_title
    , {{ json_query_string('useractivity_event_object', "'$.category_id'") }} as useractivity_discussion_block_id
    , {{ json_query_string('useractivity_event_object', "'$.category_name'") }} as useractivity_discussion_block_name
    , {{ json_query_string('useractivity_event_object', "'$.url'") }} as useractivity_discussion_page_url
    , {{ json_query_string('useractivity_event_object', "'$.query'") }} as useractivity_discussion_search_query
    , {{ json_query_string('useractivity_event_object', "'$.user_forums_roles'") }} as useractivity_discussion_roles
from course_activities
where useractivity_event_type like 'edx.forum.%'
