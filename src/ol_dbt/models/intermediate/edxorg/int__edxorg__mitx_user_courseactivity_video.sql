{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('int__edxorg__mitx_user_activity') }}
)

select
    user_username
    , courserun_readable_id
    , user_id
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_page_url
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.id'") }} as useractivity_video_id
    , case
        when lower({{ json_query_string('useractivity_event_object', "'$.duration'") }}) = 'null' then null
        else cast({{ json_query_string('useractivity_event_object', "'$.duration'") }} as decimal(38, 4))
    end as useractivity_video_duration
    , {{ json_query_string('useractivity_event_object', "'$.currentTime'") }} as useractivity_video_currenttime
    , {{ json_query_string('useractivity_event_object', "'$.old_time'") }} as useractivity_video_old_time
    , {{ json_query_string('useractivity_event_object', "'$.new_time'") }} as useractivity_video_new_time
    , {{ json_query_string('useractivity_event_object', "'$.new_speed'") }} as useractivity_video_new_speed
    , {{ json_query_string('useractivity_event_object', "'$.old_speed'") }} as useractivity_video_old_speed
from course_activities
--- Some events have url as useractivity_event_type that should be filtered as we want video events listed in
--- https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html
-- #video-interaction-events
where
    {{ regexp_like('useractivity_event_type', "'(^[\\w]+)_video'") }} = true
    or {{ regexp_like('useractivity_event_type', "'(^[\\w]+)_transcript'") }} = true
    or useractivity_event_type like 'edx.video.%'
