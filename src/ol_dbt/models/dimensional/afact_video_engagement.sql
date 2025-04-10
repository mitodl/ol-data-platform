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
        , json_query(a.useractivity_event_object, 'lax $.id' omit quotes) as video_id
        , json_query(a.useractivity_event_object, 'lax $.currentTime' omit quotes) as currenttime
        , lag(json_query(a.useractivity_event_object, 'lax $.currentTime' omit quotes), 1) 
            over (
                partition by a.user_username, a.courserun_readable_id, json_query(a.useractivity_event_object, 'lax $.id' omit quotes)  
                order by a.useractivity_timestamp desc
        ) 
            as nextcurrenttime
    from video_events as a
    left join course_content as b
        on 
            substring(b.block_id, regexp_position(b.block_id, 'block@')+6) = json_query(a.useractivity_event_object, 'lax $.id' omit quotes)
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
            'load_video'
            , 'play_video'
            , 'seek_video'
            , 'pause_video'
            , 'stop_video'
            , 'complete_video'
        )
)

, sum_mitxonline_video_events as (
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
        , max(useractivity_timestamp) as latest_activity_timestamp
        , sum(case when event_type = 'play_video' then 1 else 0 end) as video_plays
        , sum(case when event_type = 'complete_video' then 1 else 0 end) as video_completes
        , sum(case when event_type = 'play_video' then cast(nextcurrenttime as integer)-cast(currenttime as integer) else 0 end) as time_played
    from mitxonline_video_events
    group by
        video_id
        , user_username
        , openedx_user_id
        , courserun_readable_id
        , video_title
        , subsection_title
        , subsection_content_fk
        , section_title
        , section_content_fk
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
