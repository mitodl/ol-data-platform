with navigation_events as (
    select * from {{ ref('tfact_course_navigation_events') }}
)

, combined__course_runs as (
    select * from {{ ref('int__combined__course_runs') }}
)

select
    navigation_events.platform
    , combined__course_runs.course_title
    , navigation_events.courserun_readable_id
    , navigation_events.event_type
    , navigation_events.starting_position
    , navigation_events.ending_position
    , navigation_events.event_timestamp
from  navigation_events
inner join combined__course_runs
    on navigation_events.courserun_readable_id = combined__course_runs.courserun_readable_id
where navigation_events.platform in ('edxorg', 'mitxonline')
