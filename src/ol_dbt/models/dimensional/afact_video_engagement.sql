with course_content as (
    select * from {{ ref('dim_course_content') }}
)

, tfact_video_events as (
    select * from {{ ref('tfact_video_events') }}
)

, d_video as (
    select * from {{ ref('dim_video') }}
)

, start_and_end_times as (
    select
        openedx_user_id
        , courserun_readable_id
        , video_block_fk
        , max(video_position) as end_time
        , min(case when event_type = 'play_video' then video_position end) as start_time
    from tfact_video_events
    where
        event_type in (
            'play_video'
            , 'seek_video'
            , 'pause_video'
            , 'stop_video'
            , 'complete_video'
        )
    group by
        openedx_user_id
        , courserun_readable_id
        , video_block_fk
)

select
    tfact_video_events.platform
    , tfact_video_events.video_block_fk as video_id
    , tfact_video_events.openedx_user_id
    , tfact_video_events.courserun_readable_id
    , c.block_title as video_title
    , d.block_title as unit_title
    , d.content_block_pk as unit_content_fk
    , f.block_title as subsection_title
    , f.content_block_pk as subsection_content_fk
    , g.block_title as section_title
    , g.content_block_pk as section_content_fk
    , (
        cast(coalesce(start_and_end_times.end_time, '0') as decimal(30, 10))
        - cast(coalesce(start_and_end_times.start_time, '0') as decimal(30, 10))
    )
    as estimated_time_played
    , max(tfact_video_events.event_timestamp) as latest_activity_timestamp
    , sum(case when tfact_video_events.event_type = 'play_video' then 1 else 0 end) as video_played_count
    , sum(case when tfact_video_events.event_type = 'complete_video' then 1 else 0 end) as video_completed_count
from tfact_video_events
inner join d_video
    on
        tfact_video_events.video_block_fk
        = substring(d_video.video_block_pk, regexp_position(d_video.video_block_pk, 'block@') + 6)
        and tfact_video_events.courserun_readable_id = d_video.courserun_readable_id
inner join course_content as c
    on
        d_video.content_block_fk = c.content_block_pk
        and c.is_latest = true
left join course_content as d
    on
        c.parent_block_id = d.block_id
        and d.is_latest = true
left join course_content as f
    on
        d.parent_block_id = f.block_id
        and f.is_latest = true
left join course_content as g
    on
        f.parent_block_id = g.block_id
        and g.is_latest = true
left join start_and_end_times
    on
        tfact_video_events.video_block_fk = start_and_end_times.video_block_fk
        and tfact_video_events.courserun_readable_id = start_and_end_times.courserun_readable_id
        and tfact_video_events.openedx_user_id = start_and_end_times.openedx_user_id
group by
    tfact_video_events.platform
    , tfact_video_events.video_block_fk
    , tfact_video_events.openedx_user_id
    , tfact_video_events.courserun_readable_id
    , c.block_title
    , d.block_title
    , d.content_block_pk
    , f.block_title
    , f.content_block_pk
    , g.block_title
    , g.content_block_pk
    , (cast(start_and_end_times.end_time as decimal(30, 10)) - cast(start_and_end_times.start_time as decimal(30, 10)))
