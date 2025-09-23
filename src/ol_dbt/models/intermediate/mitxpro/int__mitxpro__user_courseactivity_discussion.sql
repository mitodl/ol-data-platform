{{ config(materialized="view") }}

with
    course_activities as (
        select *
        from {{ ref("stg__mitxpro__openedx__tracking_logs__user_activity") }}
        where courserun_readable_id is not null
    )

select
    user_username,
    courserun_readable_id,
    openedx_user_id,
    useractivity_event_source,
    useractivity_event_type,
    useractivity_path,
    useractivity_timestamp,
    json_query(useractivity_event_object, 'lax $.id' omit quotes) as useractivity_discussion_post_id,
    json_query(useractivity_event_object, 'lax $.title' omit quotes) as useractivity_discussion_post_title,
    json_query(useractivity_event_object, 'lax $.category_id' omit quotes) as useractivity_discussion_block_id,
    json_query(useractivity_event_object, 'lax $.category_name' omit quotes) as useractivity_discussion_block_name,
    json_query(useractivity_event_object, 'lax $.url' omit quotes) as useractivity_discussion_page_url,
    json_query(useractivity_event_object, 'lax $.query' omit quotes) as useractivity_discussion_search_query,
    json_query(useractivity_event_object, 'lax $.user_forums_roles' omit quotes) as useractivity_discussion_roles
from course_activities
where useractivity_event_type like 'edx.forum.%'
