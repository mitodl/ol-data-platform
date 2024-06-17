with micromasters_exam_grades as (
    select * from {{ ref('int__micromasters__dedp_proctored_exam_grades') }}
)

, mitxonline_exam_grades as (
    select * from {{ ref('int__mitxonline__proctored_exam_grades') }}
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

select
    course_number
    , course_title
    , examrun_readable_id as examrun_courserun_readable_id
    , user_edxorg_username
    , user_mitxonline_username
    , user_full_name
    , user_micromasters_email
    , user_mitxonline_email
    , examrun_passing_grade as proctoredexamgrade_passing_grade
    , proctoredexamgrade_percentage_grade
    , proctoredexamgrade_created_on
    , examrun_semester as semester
from micromasters_exam_grades

union all

select
    mitxonline_exam_grades.course_number
    , mitxonline_exam_grades.course_title
    , mitxonline_exam_grades.courserun_readable_id as examrun_courserun_readable_id
    , mitxonline_exam_grades.user_edxorg_username
    , mitxonline_exam_grades.user_username as user_mitxonline_username
    , mitxonline_exam_grades.user_full_name
    , micromasters_users.user_email as user_micromasters_email
    , mitxonline_exam_grades.user_email as user_mitxonline_email
    , mitxonline_exam_grades.proctoredexamgrade_passing_grade
    , mitxonline_exam_grades.proctoredexamgrade_grade as proctoredexamgrade_percentage_grade
    , mitxonline_exam_grades.proctoredexamgrade_created_on
    , mitxonline_exam_grades.semester
from mitxonline_exam_grades
left join micromasters_users on mitxonline_exam_grades.user_username = micromasters_users.user_mitxonline_username
left join micromasters_exam_grades
    on
        mitxonline_exam_grades.courserun_readable_id = micromasters_exam_grades.examrun_readable_id
        and mitxonline_exam_grades.user_username = micromasters_exam_grades.user_mitxonline_username
where micromasters_exam_grades.user_mitxonline_username is null and micromasters_exam_grades.examrun_readable_id is null
