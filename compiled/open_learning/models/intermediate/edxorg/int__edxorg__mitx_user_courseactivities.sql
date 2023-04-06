with person_courses as (
    select *
    from dev.main_staging.stg__edxorg__bigquery__mitx_person_course
    where courserun_platform = 'edX.org'
)

, runs as (
    select * from dev.main_staging.stg__edxorg__bigquery__mitx_courserun
)

, users as (
    select * from dev.main_intermediate.int__edxorg__mitx_users
)

, user_activities as (
    select
        person_courses.courserun_readable_id
        , person_courses.courseactivitiy_visited_once
        , person_courses.courseactivitiy_viewed_half
        , person_courses.courseactivitiy_num_events
        , person_courses.courseactivitiy_num_activity_days
        , person_courses.courseactivitiy_num_progress_check
        , person_courses.courseactivitiy_num_problem_check
        , person_courses.courseactivitiy_num_show_answer
        , person_courses.courseactivitiy_num_show_transcript
        , person_courses.courseactivitiy_num_seq_goto
        , person_courses.courseactivitiy_num_play_video
        , person_courses.courseactivitiy_num_seek_video
        , person_courses.courseactivitiy_num_pause_video
        , person_courses.courseactivitiy_num_video_interactions
        , person_courses.courseactivitiy_num_unique_videos_viewed
        , person_courses.courseactivitiy_percentage_total_videos_watched
        , person_courses.courseactivitiy_average_time_diff_in_sec
        , person_courses.courseactivitiy_standard_deviation_in_sec
        , person_courses.courseactivitiy_max_diff_in_sec
        , person_courses.courseactivitiy_num_consecutive_events_used
        , person_courses.courseactivitiy_total_elapsed_time_in_sec
        , person_courses.courseactivitiy_first_event_timestamp
        , person_courses.courseactivitiy_last_event_timestamp
        , users.user_id
        , users.user_email
        , users.user_username
        , runs.courserun_title
    from person_courses
    inner join users on person_courses.user_id = users.user_id
    left join runs on person_courses.courserun_readable_id = runs.courserun_readable_id
)

select * from user_activities
