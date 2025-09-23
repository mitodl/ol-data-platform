with
    showanswers as (select * from {{ ref("int__mitxonline__user_courseactivity_showanswer") }}),
    showanswers_stats as (
        select user_username, courserun_readable_id, useractivity_problem_id, count(*) as num_showanswer
        from showanswers
        group by user_username, courserun_readable_id, useractivity_problem_id
    ),
    problem_attempts as (
        select
            *,
            row_number() over (
                partition by courserun_readable_id, user_username, useractivity_problem_id
                order by useractivity_problem_attempts desc
            ) as row_num
        from {{ ref("int__mitxonline__user_courseactivity_problemcheck") }}
    ),
    most_recent_attempts as (select * from problem_attempts where row_num = 1),
    course_runs as (select * from {{ ref("int__mitxonline__course_runs") }}),
    users as (select * from {{ ref("int__mitxonline__users") }} where openedx_user_id is not null),
    combined as (
        select
            showanswers_stats.num_showanswer,
            most_recent_attempts.useractivity_problem_attempts as num_attempts,
            most_recent_attempts.useractivity_problem_success as problem_success,
            coalesce(showanswers_stats.user_username, most_recent_attempts.user_username) as user_username,
            coalesce(
                showanswers_stats.courserun_readable_id, most_recent_attempts.courserun_readable_id
            ) as courserun_readable_id,
            coalesce(
                showanswers_stats.useractivity_problem_id, most_recent_attempts.useractivity_problem_id
            ) as useractivity_problem_id
        from showanswers_stats
        full outer join
            most_recent_attempts
            on showanswers_stats.user_username = most_recent_attempts.user_username
            and showanswers_stats.courserun_readable_id = most_recent_attempts.courserun_readable_id
            and showanswers_stats.useractivity_problem_id = most_recent_attempts.useractivity_problem_id
    )

select
    combined.user_username,
    combined.courserun_readable_id,
    combined.useractivity_problem_id as problem_id,
    combined.num_showanswer,
    combined.num_attempts,
    combined.problem_success,
    users.user_full_name,
    users.user_email,
    course_runs.courserun_title,
    course_runs.course_number,
    course_runs.courserun_start_on,
    course_runs.courserun_end_on
from combined
inner join course_runs on combined.courserun_readable_id = course_runs.courserun_readable_id
left join users on combined.user_username = users.user_username
