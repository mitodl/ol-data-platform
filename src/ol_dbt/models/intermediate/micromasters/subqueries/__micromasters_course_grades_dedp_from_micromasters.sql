--- MicroMasters DEDP course combined final grades are based on course, we try to find the run with highest grade
--  for the course. If there are multiple runs with the highest grade, pick the latest grade from runs before DEDP
--  course certificates were generated

with dedp_course_grades as (
    select * from {{ ref('stg__micromasters__app__postgres__grades_combinedcoursegrade') }}
)

, courserun_grades as (
    select * from {{ ref('stg__micromasters__app__postgres__grades_courserungrade') }}
    where courserungrade_is_passing = true
)

, courseruns as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_courserun') }}
)

, courserun_grades_sorted as (
    select
        courserun_grades.user_id
        , courserun_grades.courserun_id
        , courseruns.course_id
        , row_number() over (
            partition by courserun_grades.user_id, courseruns.course_id
            --- in case of multiple highest grades, use secondary sorting to ensure the consistent result
            order by courserun_grades.courserungrade_grade desc, courserun_grades.coursegrade_created_on desc
        ) as row_num
    from courserun_grades
    inner join courseruns on courseruns.courserun_id = courserun_grades.courserun_id
    inner join dedp_course_grades
        on
            dedp_course_grades.user_id = courserun_grades.user_id
            and dedp_course_grades.course_id = courseruns.course_id
            and dedp_course_grades.coursegrade_created_on > courseruns.courserun_start_on
)

, highest_courserun_grades as (
    select *
    from courserun_grades_sorted
    where row_num = 1
)

, courses as (
    select * from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

, mm_users as (
    select * from {{ ref('__micromasters__users') }}
)

, edx_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

select
    programs.program_title
    , programs.micromasters_program_id
    , programs.mitxonline_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , courseruns.courserun_platform
    , courses.course_number
    , mm_users.user_edxorg_username
    , mm_users.user_mitxonline_username
    , mm_users.user_full_name
    , mm_users.user_address_country as user_country
    , mm_users.user_email
    , cast(dedp_course_grades.coursegrade_grade / 100 as decimal(5, 3)) as coursegrade_grade
    , dedp_course_grades.coursegrade_created_on
from dedp_course_grades
inner join highest_courserun_grades
    on
        dedp_course_grades.user_id = highest_courserun_grades.user_id
        and dedp_course_grades.course_id = highest_courserun_grades.course_id
inner join courseruns on highest_courserun_grades.courserun_id = courseruns.courserun_id
inner join courses on dedp_course_grades.course_id = courses.course_id
inner join programs on courses.program_id = programs.micromasters_program_id
inner join mm_users on dedp_course_grades.user_id = mm_users.user_id
---not all MM users can match with edx using edxorg username
left join edx_users on edx_users.user_username = mm_users.user_edxorg_username
