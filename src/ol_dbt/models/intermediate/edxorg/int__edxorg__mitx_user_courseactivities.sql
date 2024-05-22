with course_activities as (
    select * from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

, users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, course_activities_video as (
    select * from {{ ref('int__edxorg__user_courseactivity_video') }}
)

, problem_check as (
    select * from {{ ref('int__edxorg__user_courseactivity_problemcheck') }}
)

, problem_check_stats as (
    select
        user_username
        , courserun_readable_id
        , max(useractivity_timestamp) as courseactivity_last_problem_check_timestamp
    from problem_check
    group by user_username, courserun_readable_id
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
        course_activities.user_username
        , course_activities.courserun_readable_id
        , coalesce(users.openedx_user_id, course_activities.openedx_user_id) as openedx_user_id
        , count(distinct date(from_iso8601_timestamp(course_activities.useractivity_timestamp)))
        as courseactivity_num_days_activity
        , count(*) as courseactivity_num_events
        , min(course_activities.useractivity_timestamp) as courseactivity_first_event_timestamp
        , max(course_activities.useractivity_timestamp) as courseactivity_last_event_timestamp
    from course_activities
    left join users on course_activities.user_username = users.user_username
    group by
        course_activities.user_username
        , coalesce(users.openedx_user_id, course_activities.openedx_user_id)
        , course_activities.courserun_readable_id
)

select
    all_course_activities_stats.openedx_user_id
    , all_course_activities_stats.user_username
    , all_course_activities_stats.courserun_readable_id
    , all_course_activities_stats.courseactivity_num_days_activity
    , all_course_activities_stats.courseactivity_num_events
    , play_video_stats.courseactivity_num_unique_play_video
    , play_video_stats.courseactivity_num_play_video
    , play_video_stats.courseactivity_last_play_video_timestamp
    , problem_check_stats.courseactivity_last_problem_check_timestamp
    , all_course_activities_stats.courseactivity_first_event_timestamp
    , all_course_activities_stats.courseactivity_last_event_timestamp
from all_course_activities_stats
left join play_video_stats
    on
        all_course_activities_stats.user_username = play_video_stats.user_username
        and all_course_activities_stats.courserun_readable_id = play_video_stats.courserun_readable_id
left join problem_check_stats
    on
        all_course_activities_stats.user_username = problem_check_stats.user_username
        and all_course_activities_stats.courserun_readable_id = problem_check_stats.courserun_readable_id
