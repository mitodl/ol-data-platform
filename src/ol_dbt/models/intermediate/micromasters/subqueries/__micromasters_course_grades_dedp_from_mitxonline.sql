with courserun_grades as (
    select * from {{ ref('int__mitxonline__courserun_grades') }}
)

, mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)


, courseruns as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, courses as (
    select * from {{ ref('int__mitxonline__courses') }}
)

, enrollments_with_program as (
    select * from {{ ref('int__mitxonline__courserunenrollments_with_programs') }}
)

select
    enrollments_with_program.program_title
    , enrollments_with_program.micromasters_program_id
    , enrollments_with_program.mitxonline_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , courseruns.courserun_platform
    , courses.course_number
    , micromasters_users.user_edxorg_username
    , mitxonline_users.user_username as user_mitxonline_username
    , mitxonline_users.user_full_name
    , mitxonline_users.user_address_country as user_country
    , mitxonline_users.user_email
    , courserun_grades.courserungrade_grade
    , courserun_grades.courserungrade_is_passing
    , courserun_grades.courserungrade_created_on
from courserun_grades
inner join courseruns on courserun_grades.courserun_id = courseruns.courserun_id
inner join courses on courserun_grades.course_id = courses.course_id
inner join enrollments_with_program
    on
        enrollments_with_program.courserun_id = courseruns.courserun_id
        and enrollments_with_program.user_id = courserun_grades.user_id
inner join mitxonline_users on courserun_grades.user_id = mitxonline_users.user_id
left join micromasters_users
    on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
left join edx_users on edx_users.user_username = micromasters_users.user_edxorg_username
where enrollments_with_program.is_dedp_program = true
