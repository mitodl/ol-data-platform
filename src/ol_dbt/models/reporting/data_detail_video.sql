with
    video_events as (select * from {{ ref("tfact_video_events") }}),
    videos as (select * from {{ ref("dim_video") }}),
    combined__course_runs as (select * from {{ ref("int__combined__course_runs") }})

select
    video_events.platform,
    combined__course_runs.course_title,
    video_events.courserun_readable_id,
    video_events.event_type,
    video_events.video_duration,
    video_events.video_position,
    video_events.event_timestamp,
    videos.video_name
from video_events
inner join
    videos
    on video_events.courserun_readable_id = videos.courserun_readable_id
    and video_events.video_block_fk = substring(videos.video_block_pk, strpos(videos.video_block_pk, 'block@') + 6)
inner join combined__course_runs on video_events.courserun_readable_id = combined__course_runs.courserun_readable_id
