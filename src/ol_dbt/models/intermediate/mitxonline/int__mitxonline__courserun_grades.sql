-- Course Grade information for MITx Online

with grades as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserungrade') }}
)

, runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, courserun_grades as (
    select
        grades.courserungrade_id
        , grades.courserun_id
        , runs.course_id
        , runs.courserun_title
        , runs.courserun_readable_id
        , runs.courserun_platform
        , runs.courserun_url
        , grades.courserungrade_grade
        , grades.courserungrade_letter_grade
        , grades.courserungrade_is_passing
        , grades.courserungrade_created_on
        , grades.courserungrade_updated_on
        , grades.user_id
        , users.user_username
        , users.user_edxorg_username
        , users.user_email
        , users.user_full_name
    from grades
    inner join runs on grades.courserun_id = runs.courserun_id
    inner join users on grades.user_id = users.user_id
)

select * from courserun_grades
