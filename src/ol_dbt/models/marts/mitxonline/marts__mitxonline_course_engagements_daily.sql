with
    course_activities_daily as (select * from {{ ref("int__mitxonline__user_courseactivities_daily") }}),
    users as (select * from {{ ref("int__mitxonline__users") }} where openedx_user_id is not null),
    video as (select * from {{ ref("int__mitxonline__user_courseactivity_video") }}),
    problem_submitted as (select * from {{ ref("int__mitxonline__user_courseactivity_problemsubmitted") }}),
    discussion as (select * from {{ ref("int__mitxonline__user_courseactivity_discussion") }}),
    course_runs as (select * from {{ ref("int__mitxonline__course_runs") }}),
    problem_submitted_users_daily as (
        select
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date,
            count(*) as num_problem_submitted
        from problem_submitted
        group by user_username, courserun_readable_id, date(from_iso8601_timestamp(useractivity_timestamp))
    ),
    played_video_users_daily as (
        select
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date,
            count(*) as num_video_played
        from video
        where useractivity_event_type = 'play_video'
        group by user_username, courserun_readable_id, date(from_iso8601_timestamp(useractivity_timestamp))
    ),
    participated_discussion_users_daily as (
        select
            user_username,
            courserun_readable_id,
            date(from_iso8601_timestamp(useractivity_timestamp)) as courseactivity_date,
            count(*) as num_discussion_participated
        from discussion
        -- edx.forum.comment.created, edx.forum.response.created, edx.forum.thread.created
        where useractivity_event_type like 'edx.forum.%.created'
        group by user_username, courserun_readable_id, date(from_iso8601_timestamp(useractivity_timestamp))
    )

select
    course_activities_daily.courseactivity_date,
    course_activities_daily.user_username,
    users.user_full_name,
    users.user_email,
    course_activities_daily.courserun_readable_id,
    course_runs.courserun_title,
    course_runs.course_number,
    course_runs.courserun_start_on,
    course_runs.courserun_end_on,
    course_activities_daily.courseactivity_num_events as num_events,
    problem_submitted_users_daily.num_problem_submitted,
    played_video_users_daily.num_video_played,
    participated_discussion_users_daily.num_discussion_participated
from course_activities_daily
inner join course_runs on course_activities_daily.courserun_readable_id = course_runs.courserun_readable_id
left join users on course_activities_daily.user_username = users.user_username
left join
    problem_submitted_users_daily
    on course_activities_daily.courseactivity_date = problem_submitted_users_daily.courseactivity_date
    and course_activities_daily.courserun_readable_id = problem_submitted_users_daily.courserun_readable_id
    and course_activities_daily.user_username = problem_submitted_users_daily.user_username
left join
    played_video_users_daily
    on course_activities_daily.courseactivity_date = played_video_users_daily.courseactivity_date
    and course_activities_daily.courserun_readable_id = played_video_users_daily.courserun_readable_id
    and course_activities_daily.user_username = played_video_users_daily.user_username
left join
    participated_discussion_users_daily
    on course_activities_daily.courseactivity_date = participated_discussion_users_daily.courseactivity_date
    and course_activities_daily.courserun_readable_id = participated_discussion_users_daily.courserun_readable_id
    and course_activities_daily.user_username = participated_discussion_users_daily.user_username
