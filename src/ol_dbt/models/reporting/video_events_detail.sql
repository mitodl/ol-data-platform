{{
    config(
        materialized='view'
    )
}}

{#
    Reporting view for video events with denormalized course and video information.
    Replaces Superset virtual dataset: Data_Detail_Video
#}

with video_events as (
    select * from {{ ref('tfact_video_events') }}
)

, course_runs as (
    select
        courserun_readable_id
        , courserun_title as course_title
    from {{ ref('int__combined__course_runs') }}
)

, videos as (
    select
        courserun_readable_id
        , video_block_pk
        , video_name
    from {{ ref('dim_video') }}
)

select
    video_events.platform
    , course_runs.course_title
    , video_events.courserun_readable_id
    , video_events.event_type
    , video_events.video_duration
    , video_events.video_position
    , video_events.event_timestamp
    , videos.video_name
    , video_events.user_username
    , video_events.openedx_user_id
    , video_events.user_fk
from video_events
left join videos
    on video_events.courserun_readable_id = videos.courserun_readable_id
    and video_events.video_block_fk = substring(videos.video_block_pk from regexp_position(videos.video_block_pk, 'block@') + 6)
left join course_runs
    on video_events.courserun_readable_id = course_runs.courserun_readable_id
