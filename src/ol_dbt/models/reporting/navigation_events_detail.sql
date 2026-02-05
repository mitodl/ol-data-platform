{{
    config(
        materialized='view'
    )
}}

{#
    Reporting view for navigation events with denormalized course information.
    Replaces Superset virtual dataset: Data_Detail_Nav
#}

with navigation_events as (
    select * from {{ ref('tfact_course_navigation_events') }}
)

, course_runs as (
    select
        courserun_readable_id
        , courserun_title as course_title
    from {{ ref('int__combined__course_runs') }}
)

select
    navigation_events.platform
    , course_runs.course_title
    , navigation_events.courserun_readable_id
    , navigation_events.event_type
    , navigation_events.starting_position
    , navigation_events.ending_position
    , navigation_events.event_timestamp
    , navigation_events.user_username
    , navigation_events.openedx_user_id
    , navigation_events.user_fk
from navigation_events
left join course_runs
    on navigation_events.courserun_readable_id = course_runs.courserun_readable_id
where navigation_events.platform in ('edxorg', 'mitxonline')
