{{ config(materialized='view') }}
with course_activities as (
    select * from {{ ref('__mitxpro__user_courseactivities') }}
)

select
    user_username
    , courserun_readable_id
    , openedx_user_id
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_event_name
    , useractivity_event_object
    , useractivity_timestamp
    , json_query(useractivity_event_object, 'lax $.id' omit quotes) as useractivity_video_id
    , json_query(useractivity_event_object, 'lax $.duration' omit quotes) as useractivity_video_duration
    , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as useractivity_video_currenttime
from course_activities
where useractivity_event_type in (
    'load_video', 'play_video', 'pause_video', 'stop_video', 'seek_video'
    , 'speed_change_video', 'show_transcript', 'hide_transcript'
    , 'edx.video.closed_captions.hidden', 'edx.video.closed_captions.shown'
)
