{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select
    user_username
    , courserun_readable_id
    , user_id
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_page_url
    , useractivity_timestamp
    , json_query(useractivity_event_object, 'lax $.id' omit quotes) as useractivity_video_id
    , json_query(useractivity_event_object, 'lax $.duration' omit quotes) as useractivity_video_duration
    , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as useractivity_video_currenttime
    , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as useractivity_video_old_time
    , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as useractivity_video_new_time
    , json_query(useractivity_event_object, 'lax $.new_speed' omit quotes) as useractivity_video_new_speed
    , json_query(useractivity_event_object, 'lax $.old_speed' omit quotes) as useractivity_video_old_speed
from course_activities
--- Some events have url as useractivity_event_type that should be filtered as we want video events listed in
--- https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html
-- #video-interaction-events
where
    regexp_like(useractivity_event_type, '(^[\w]+)_video') = true
    or regexp_like(useractivity_event_type, '(^[\w]+)_transcript') = true
    or useractivity_event_type like 'edx.video.%'
