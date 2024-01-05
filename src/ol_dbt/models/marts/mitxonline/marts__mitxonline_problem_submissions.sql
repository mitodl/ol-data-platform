with problem_response as (
    select
        *
        , row_number() over (
            partition by courserun_readable_id, user_username, useractivity_problem_id
            order by useractivity_problem_attempts desc
        ) as most_recent_num
    from {{ ref('int__mitxonline__user_courseactivity_problemcheck') }}
)

, course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    problem_response.user_username
    , problem_response.courserun_readable_id
    , problem_response.useractivity_problem_id as problem_id
    , problem_response.useractivity_problem_name as problem_name
    , problem_response.useractivity_problem_attempts as num_attempts
    , problem_response.useractivity_problem_student_answers as student_answers
    , problem_response.useractivity_problem_success as problem_success
    , problem_response.useractivity_problem_current_grade as problem_grade
    , problem_response.useractivity_problem_max_grade as problem_max_grade
    , problem_response.useractivity_timestamp as problem_submission_timestamp
    , users.user_full_name
    , users.user_email
    , course_runs.courserun_title
    , course_runs.course_number
    , course_runs.courserun_start_on
    , course_runs.courserun_end_on
    , if(problem_response.most_recent_num = 1, true, false) as is_most_recent_attempt
from problem_response
inner join course_runs on problem_response.courserun_readable_id = course_runs.courserun_readable_id
left join users on problem_response.user_username = users.user_username
