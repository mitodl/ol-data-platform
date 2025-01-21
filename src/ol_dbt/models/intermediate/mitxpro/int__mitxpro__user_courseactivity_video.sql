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
    , useractivity_page_url
    , useractivity_timestamp
    , json_query(useractivity_event_object, 'lax $.id' omit quotes) as useractivity_video_id
    , case
        when lower(json_query(useractivity_event_object, 'lax $.duration' omit quotes)) = 'null' then null
        else cast(json_query(useractivity_event_object, 'lax $.duration' omit quotes) as decimal(38, 4))
    end as useractivity_video_duration
    , json_query(useractivity_event_object, 'lax $.currentTime' omit quotes) as useractivity_video_currenttime
    , json_query(useractivity_event_object, 'lax $.old_time' omit quotes) as useractivity_video_old_time
    , json_query(useractivity_event_object, 'lax $.new_time' omit quotes) as useractivity_video_new_time
    , json_query(useractivity_event_object, 'lax $.new_speed' omit quotes) as useractivity_video_new_speed
    , json_query(useractivity_event_object, 'lax $.old_speed' omit quotes) as useractivity_video_old_speed
from course_activities
where useractivity_event_type like '%\_video' escape '\' or useractivity_event_type like 'edx.video.%' --noqa
