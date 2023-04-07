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

, programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, program_to_courses as (
    select * from {{ ref('int__mitxonline__program_to_courses') }}
)

, micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

----program title and id are different between MM and MITxOnline, use title from MM
select
    micromasters_programs.program_title
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
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
inner join program_to_courses on program_to_courses.course_id = courses.course_id
inner join programs on program_to_courses.program_id = programs.program_id
inner join mitxonline_users on courserun_grades.user_id = mitxonline_users.user_id
inner join micromasters_programs
    on micromasters_programs.program_id = {{ var("dedp_micromasters_program_id") }}
left join micromasters_users
    on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
left join edx_users on edx_users.user_username = micromasters_users.user_edxorg_username
where programs.program_id = {{ var("dedp_mitxonline_program_id") }}
