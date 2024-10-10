with combined_problem_checks as (
    select
        '{{ var("mitxonline") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_problem_id
        , useractivity_problem_name
        , useractivity_problem_student_answers
        , useractivity_problem_attempts
        , useractivity_problem_success
        , useractivity_problem_current_grade
        , useractivity_problem_max_grade
        , useractivity_timestamp
    from {{ ref('int__mitxonline__user_courseactivity_problemcheck') }}

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_problem_id
        , useractivity_problem_name
        , useractivity_problem_student_answers
        , useractivity_problem_attempts
        , useractivity_problem_success
        , useractivity_problem_current_grade
        , useractivity_problem_max_grade
        , useractivity_timestamp
    from {{ ref('int__edxorg__mitx_user_courseactivity_problemcheck') }}

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_problem_id
        , useractivity_problem_name
        , useractivity_problem_student_answers
        , useractivity_problem_attempts
        , useractivity_problem_success
        , useractivity_problem_current_grade
        , useractivity_problem_max_grade
        , useractivity_timestamp
    from {{ ref('int__mitxpro__user_courseactivity_problemcheck') }}

    union all

    select
        '{{ var("residential") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_problem_id
        , useractivity_problem_name
        , useractivity_problem_student_answers
        , useractivity_problem_attempts
        , useractivity_problem_success
        , useractivity_problem_current_grade
        , useractivity_problem_max_grade
        , useractivity_timestamp
    from {{ ref('int__mitxresidential__user_courseactivity_problemcheck') }}
)

, combined_runs as (
    select * from {{ ref('int__combined__course_runs') }}
)

, combined_users as (
    select * from {{ ref('int__combined__users') }}

)

select
    combined_problem_checks.platform
    , combined_problem_checks.user_username
    , combined_problem_checks.courserun_readable_id
    , combined_problem_checks.useractivity_problem_id as problem_id
    , combined_problem_checks.useractivity_problem_name as problem_name
    , combined_problem_checks.useractivity_problem_attempts as num_attempts
    , combined_problem_checks.useractivity_problem_student_answers as student_answers
    , combined_problem_checks.useractivity_problem_success as problem_success
    , combined_problem_checks.useractivity_problem_current_grade as problem_grade
    , combined_problem_checks.useractivity_problem_max_grade as problem_max_grade
    , combined_problem_checks.useractivity_timestamp as problem_submission_timestamp
    , combined_users.user_hashed_id
    , combined_users.user_full_name
    , combined_users.user_email
    , combined_users.user_address_country as user_country_code
    , combined_users.user_highest_education
    , combined_users.user_gender
    , combined_runs.courserun_title
    , combined_runs.course_readable_id
    , combined_runs.courserun_is_current
    , combined_runs.courserun_start_on
    , combined_runs.courserun_end_on
from combined_problem_checks
inner join combined_runs
    on
        combined_problem_checks.courserun_readable_id = combined_runs.courserun_readable_id
        and combined_problem_checks.platform = combined_runs.platform
left join combined_users
    on
        combined_problem_checks.user_username = combined_users.user_username
        and combined_problem_checks.platform = combined_users.platform
