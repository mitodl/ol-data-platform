with
    courserun_grades as (select * from {{ ref("int__mitxonline__courserun_grades") }}),
    courseruns as (select * from {{ ref("int__mitxonline__course_runs") }}),
    courses as (select * from {{ ref("int__mitxonline__courses") }}),
    enrollments_with_program as (select * from {{ ref("int__mitxonline__courserunenrollments_with_programs") }})

select
    enrollments_with_program.program_title,
    enrollments_with_program.micromasters_program_id,
    enrollments_with_program.mitxonline_program_id,
    courseruns.courserun_title,
    courseruns.courserun_readable_id,
    courseruns.courserun_platform,
    courses.course_number,
    courserun_grades.user_username as user_mitxonline_username,
    courserun_grades.user_id as user_mitxonline_id,
    courserun_grades.courserungrade_grade,
    courserun_grades.courserungrade_is_passing,
    courserun_grades.courserungrade_created_on
from courserun_grades
inner join courseruns on courserun_grades.courserun_id = courseruns.courserun_id
inner join courses on courserun_grades.course_id = courses.course_id
inner join
    enrollments_with_program
    on courseruns.courserun_id = enrollments_with_program.courserun_id
    and courserun_grades.user_id = enrollments_with_program.user_id
where enrollments_with_program.is_dedp_program = true
