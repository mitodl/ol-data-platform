with
    video_events as (select * from {{ ref("tfact_video_events") }}),
    videos as (select * from {{ ref("dim_video") }})

-- dim_course_run and dim_course are SCD2 and can hold more than one is_current = true row per
-- key, so keep the latest. Runs with no dim_course row (all residential runs) use the run title.
, course_runs as (
    select
        courserun_readable_id
        , platform
        , course_title
    from (
        select
            dim_course_run.courserun_readable_id
            , dim_course_run.platform
            , coalesce(dim_course.course_title, dim_course_run.courserun_title) as course_title
            , row_number() over (
                partition by dim_course_run.platform, dim_course_run.courserun_readable_id
                order by
                    dim_course_run.effective_date desc nulls last
                    , dim_course.effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }} as dim_course_run
        left join {{ ref('dim_course') }} as dim_course
            on
                dim_course_run.course_fk = dim_course.course_pk
                and dim_course.is_current = true
        where dim_course_run.is_current = true
    )
    where _row_num = 1
)

select
    video_events.platform,
    course_runs.course_title,
    video_events.courserun_readable_id,
    video_events.user_username,
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
inner join
    course_runs
    on video_events.courserun_readable_id = course_runs.courserun_readable_id
    and video_events.platform = course_runs.platform
group by
    video_events.platform,
    course_runs.course_title,
    video_events.courserun_readable_id,
    video_events.user_username,
    video_events.event_type,
    video_events.video_duration,
    video_events.video_position,
    video_events.event_timestamp,
    videos.video_name
