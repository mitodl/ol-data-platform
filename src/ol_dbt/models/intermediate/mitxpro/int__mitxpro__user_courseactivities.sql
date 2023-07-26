-- xPro user activities from tracking logs
with course_activities as (
    select * from {{ ref('__mitxpro__user_courseactivities') }}
)

, course_activities_video as (
    select * from {{ ref('__mitxpro__user_courseactivity_video') }}
)

, play_video_stats as (
    select
        user_username
        , courserun_readable_id
        , count(distinct useractivity_video_id) as courseactivity_num_unique_play_video
        , count(*) as courseactivity_num_play_video
        , max(useractivity_timestamp) as courseactivity_last_play_video_timestamp
    from course_activities_video
    where useractivity_event_type = 'play_video'
    group by user_username, courserun_readable_id
)

, all_course_activities_stats as (
    select
        user_username
        , courserun_readable_id
        , count(distinct date(from_iso8601_timestamp(useractivity_timestamp))) as courseactivity_num_days_activity
        , count(*) as courseactivity_num_events
        , min(useractivity_timestamp) as courseactivity_first_event_timestamp
        , max(useractivity_timestamp) as courseactivity_last_event_timestamp
    from course_activities
    group by user_username, courserun_readable_id
)

select
    all_course_activities_stats.user_username
    , all_course_activities_stats.courserun_readable_id
    , all_course_activities_stats.courseactivity_num_days_activity
    , all_course_activities_stats.courseactivity_num_events
    , play_video_stats.courseactivity_num_unique_play_video
    , play_video_stats.courseactivity_num_play_video
    , play_video_stats.courseactivity_last_play_video_timestamp
    , all_course_activities_stats.courseactivity_first_event_timestamp
    , all_course_activities_stats.courseactivity_last_event_timestamp
from all_course_activities_stats
left join play_video_stats on
    all_course_activities_stats.user_username = play_video_stats.user_username
    and all_course_activities_stats.courserun_readable_id = play_video_stats.courserun_readable_id
