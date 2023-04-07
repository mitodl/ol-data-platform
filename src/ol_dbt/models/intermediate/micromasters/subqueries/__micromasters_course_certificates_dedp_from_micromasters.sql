--- Unlike other platforms, DEDP course certificate from MM is based on course not run, we try to match it with
--- course run from learner's highest grades
with dedp_course_certificates as (
    select * from {{ ref('stg__micromasters__app__postgres__grades_coursecertificate') }}
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
            order by courserun_grades.courserungrade_grade desc
        ) as row_num
    from courserun_grades
    inner join courseruns on courseruns.courserun_id = courserun_grades.courserun_id
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
    select * from {{ ref('stg__micromasters__app__postgres__courses_program') }}
)

, mm_users as (
    select * from {{ ref('__micromasters__users') }}
)


select
    programs.program_title
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , courses.course_number
    , mm_users.user_edxorg_username
    , mm_users.user_mitxonline_username
    , mm_users.user_email
    , mm_users.user_full_name
    , mm_users.user_address_country as user_country
    , dedp_course_certificates.coursecertificate_created_on
from dedp_course_certificates
inner join highest_courserun_grades
    on
        dedp_course_certificates.user_id = highest_courserun_grades.user_id
        and dedp_course_certificates.course_id = highest_courserun_grades.course_id
inner join courseruns on highest_courserun_grades.courserun_id = courseruns.courserun_id
inner join courses on dedp_course_certificates.course_id = courses.course_id
inner join programs on courses.program_id = programs.program_id
inner join mm_users on dedp_course_certificates.user_id = mm_users.user_id
