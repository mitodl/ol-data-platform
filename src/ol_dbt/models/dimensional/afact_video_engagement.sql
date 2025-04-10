with course_content as (
    select * from {{ ref('dim_course_content') }}
)

, video_events as (
    select * from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
)

, mitxonline_video_events as (
    select 
        a.user_username
        , a.openedx_user_id
        , a.courserun_readable_id
        , a.useractivity_event_type as event_type
        , a.useractivity_event_object as event_json
        , a.useractivity_timestamp
        , b.block_title as video_title
        , c.block_title as subsection_title
        , c.content_block_pk as subsection_content_fk
        , d.block_title as section_title
        , d.content_block_pk as section_content_fk
        , replace(json_query(a.useractivity_event_object, 'lax $.id'), '"', '') as video_id
        , json_query(a.useractivity_event_object, 'lax $.currentTime') as currenttime
    from video_events as a
    left join course_content as b
        on 
            substring(b.block_id, regexp_position(b.block_id, 'block@') + 6) 
            = replace(json_query(a.useractivity_event_object, 'lax $.id'), '"', '')
            and a.courserun_readable_id = b.courserun_readable_id
            and b.is_latest = true
            and b.block_category = 'video'
    left join course_content as c
        on 
            b.parent_block_id = c.block_id
            and c.is_latest = true
            and c.block_category = 'vertical'
    left join course_content as d
        on 
            c.parent_block_id = d.block_id
            and d.is_latest = true
            and d.block_category = 'sequential'
    where 
        a.courserun_readable_id is not null
        and a.useractivity_event_type in (
            'play_video'
            , 'seek_video'
            , 'pause_video'
            , 'stop_video'
            , 'complete_video'
        )
)

, start_and_end_times as (
    select 
        openedx_user_id
        , courserun_readable_id
        , video_id
        , min(case when event_type = 'play_video' then currenttime end) as start_time
        , max(case when event_type <> 'play_video' then currenttime end) as end_time
    from mitxonline_video_events
    group by 
        openedx_user_id
        , courserun_readable_id
        , video_id
)

, sum_mitxonline_video_events as (
    select
        mitxonline_video_events.video_id
        , mitxonline_video_events.user_username
        , mitxonline_video_events.openedx_user_id
        , mitxonline_video_events.courserun_readable_id
        , mitxonline_video_events.video_title
        , mitxonline_video_events.subsection_title
        , mitxonline_video_events.subsection_content_fk
        , mitxonline_video_events.section_title
        , mitxonline_video_events.section_content_fk
        , (
            cast(start_and_end_times.end_time as decimal(30, 10)) 
            - cast(start_and_end_times.start_time as decimal(30, 10))
        ) as time_played
        , max(mitxonline_video_events.useractivity_timestamp) as latest_activity_timestamp
        , sum(case when mitxonline_video_events.event_type = 'play_video' then 1 else 0 end) as video_plays
        , sum(case when mitxonline_video_events.event_type = 'complete_video' then 1 else 0 end) as video_completes
    from mitxonline_video_events
    left join start_and_end_times
        on 
            mitxonline_video_events.video_id = start_and_end_times.video_id
            and mitxonline_video_events.courserun_readable_id = start_and_end_times.courserun_readable_id
            and mitxonline_video_events.openedx_user_id = start_and_end_times.openedx_user_id
            and start_and_end_times.end_time > start_and_end_times.start_time
    group by
        mitxonline_video_events.video_id
        , mitxonline_video_events.user_username
        , mitxonline_video_events.openedx_user_id
        , mitxonline_video_events.courserun_readable_id
        , mitxonline_video_events.video_title
        , mitxonline_video_events.subsection_title
        , mitxonline_video_events.subsection_content_fk
        , mitxonline_video_events.section_title
        , mitxonline_video_events.section_content_fk
        , (
            cast(start_and_end_times.end_time as decimal(30, 10)) 
            - cast(start_and_end_times.start_time as decimal(30, 10))
        )
)

select
    video_id
    , user_username
    , openedx_user_id
    , courserun_readable_id
    , video_title
    , subsection_title
    , subsection_content_fk
    , section_title
    , section_content_fk
    , time_played as estimated_time_played
    , latest_activity_timestamp
    , case when video_plays > 0 then 1 else 0 end as video_played_count
    , case when video_completes > 0 then 1 else 0 end as video_completed_count
from sum_mitxonline_video_events
