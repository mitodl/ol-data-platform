-- xPro user activities from tracking logs
with
    course_activities as (
        select *
        from {{ ref("stg__mitxpro__openedx__tracking_logs__user_activity") }}
        where courserun_readable_id is not null
    ),
    course_activities_video as (select * from {{ ref("int__mitxpro__user_courseactivity_video") }}),
    problem_check as (select * from {{ ref("int__mitxpro__user_courseactivity_problemcheck") }}),
    users as (select * from {{ ref("int__mitxpro__users") }}),
    student_module as (select * from {{ ref("stg__mitxpro__openedx__mysql__courseware_studentmodule") }}),
    course_structure as (select * from {{ ref("int__mitxpro__course_structure") }}),
    course_chapters_stats as (
        select
            student_module.openedx_user_id,
            student_module.courserun_readable_id,
            count(distinct course_structure.coursestructure_chapter_id) as courseactivity_num_chapters_visited
        from student_module
        inner join
            course_structure
            on student_module.courserun_readable_id = course_structure.courserun_readable_id
            and student_module.coursestructure_block_id = course_structure.coursestructure_block_id
        group by student_module.openedx_user_id, student_module.courserun_readable_id
    ),
    play_video_stats as (
        select
            user_username,
            courserun_readable_id,
            count(distinct useractivity_video_id) as courseactivity_num_unique_play_video,
            count(*) as courseactivity_num_play_video,
            max(useractivity_timestamp) as courseactivity_last_play_video_timestamp
        from course_activities_video
        where useractivity_event_type = 'play_video'
        group by user_username, courserun_readable_id
    ),
    problem_check_stats as (
        select
            user_username,
            courserun_readable_id,
            max(useractivity_timestamp) as courseactivity_last_problem_check_timestamp
        from problem_check
        group by user_username, courserun_readable_id
    ),
    all_course_activities_stats as (
        select
            course_activities.user_username,
            course_activities.courserun_readable_id,
            coalesce(users.openedx_user_id, course_activities.openedx_user_id) as openedx_user_id,
            count(
                distinct date(from_iso8601_timestamp(course_activities.useractivity_timestamp))
            ) as courseactivity_num_days_activity,
            count(*) as courseactivity_num_events,
            min(course_activities.useractivity_timestamp) as courseactivity_first_event_timestamp,
            max(course_activities.useractivity_timestamp) as courseactivity_last_event_timestamp
        from course_activities
        left join users on course_activities.user_username = users.user_username
        group by
            course_activities.user_username,
            coalesce(users.openedx_user_id, course_activities.openedx_user_id),
            course_activities.courserun_readable_id
    )

select
    all_course_activities_stats.user_username,
    all_course_activities_stats.openedx_user_id,
    all_course_activities_stats.courserun_readable_id,
    all_course_activities_stats.courseactivity_num_days_activity,
    all_course_activities_stats.courseactivity_num_events,
    play_video_stats.courseactivity_num_unique_play_video,
    play_video_stats.courseactivity_num_play_video,
    play_video_stats.courseactivity_last_play_video_timestamp,
    problem_check_stats.courseactivity_last_problem_check_timestamp,
    all_course_activities_stats.courseactivity_first_event_timestamp,
    all_course_activities_stats.courseactivity_last_event_timestamp,
    course_chapters_stats.courseactivity_num_chapters_visited
from all_course_activities_stats
left join
    play_video_stats
    on all_course_activities_stats.user_username = play_video_stats.user_username
    and all_course_activities_stats.courserun_readable_id = play_video_stats.courserun_readable_id
left join
    problem_check_stats
    on all_course_activities_stats.user_username = problem_check_stats.user_username
    and all_course_activities_stats.courserun_readable_id = problem_check_stats.courserun_readable_id
left join
    course_chapters_stats
    on all_course_activities_stats.openedx_user_id = course_chapters_stats.openedx_user_id
    and all_course_activities_stats.courserun_readable_id = course_chapters_stats.courserun_readable_id
