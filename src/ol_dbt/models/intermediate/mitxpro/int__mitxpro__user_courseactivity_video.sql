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
    , useractivity_timestamp
    , json_query(useractivity_event_object, 'lax $.id' omit quotes) as useractivity_video_id
    , json_query(useractivity_event_object, 'lax $.duration' omit quotes) as useractivity_video_duration
    , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as useractivity_video_currenttime
from course_activities
where useractivity_event_type in ('play_video', 'pause_video', 'stop_video')
