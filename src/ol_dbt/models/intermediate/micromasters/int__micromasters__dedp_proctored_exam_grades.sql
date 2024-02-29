with exam_grades as (
    select *
    from {{ ref('stg__micromasters__app__postgres__grades_proctoredexamgrade') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

, courses as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, mm_users as (
    select * from {{ ref('__micromasters__users') }}
)

, mixonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    programs.program_title
    , programs.micromasters_program_id
    , programs.mitxonline_program_id
    , courses.course_title
    , courses.course_number
    , mm_users.user_edxorg_username
    , mm_users.user_mitxonline_username
    , mm_users.user_full_name
    , mm_users.user_micromasters_email
    , mixonline_users.user_email as user_mitxonline_email
    , exam_grades.proctoredexamgrade_score
    , exam_grades.proctoredexamgrade_is_passing
    , exam_grades.proctoredexamgrade_passing_score
    , exam_grades.proctoredexamgrade_percentage_grade
    , exam_grades.proctoredexamgrade_exam_on
    , exam_grades.proctoredexamgrade_created_on
    , exam_grades.proctoredexamgrade_updated_on
    , exam_grades.proctoredexamgrade_id
from exam_grades
inner join courses on exam_grades.course_id = courses.course_id
inner join programs on courses.program_id = programs.micromasters_program_id
inner join mm_users on exam_grades.user_id = mm_users.user_id
left join mixonline_users on mm_users.user_mitxonline_username = mixonline_users.user_username
