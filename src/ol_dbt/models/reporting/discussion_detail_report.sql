with
    discussion_events as (select * from {{ ref("tfact_discussion_events") }})

    , course_runs as (select * from {{ ref("int__combined__course_runs") }})

select
    discussion_events.platform
    , course_runs.course_title
    , discussion_events.courserun_readable_id
    , discussion_events.event_type
    , discussion_events.post_title
    , discussion_events.post_content
    , discussion_events.discussion_component_name
    , discussion_events.page_url
    , discussion_events.event_timestamp
from discussion_events
inner join course_runs
    on discussion_events.courserun_readable_id = course_runs.courserun_readable_id
