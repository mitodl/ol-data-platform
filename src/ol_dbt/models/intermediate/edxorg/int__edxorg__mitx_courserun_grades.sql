-- Course Run Grades information from edx.org

with grades as (
    select
        user_id
        , courserun_readable_id
        , courserungrade_passing_grade
        , courserungrade_user_grade
        , courserungrade_is_passing
    from {{ ref('stg__edxorg__bigquery__mitx_person_course') }}
    where courserun_platform = '{{ var("edxorg") }}'
)

, runs as (
    select * from {{ ref('stg__edxorg__bigquery__mitx_courserun') }}
)

, users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, edxorg_grades as (
    select
        grades.courserun_readable_id
        , grades.courserungrade_passing_grade
        , grades.courserungrade_user_grade
        , grades.courserungrade_is_passing
        , users.user_id
        , users.user_email
        , users.user_username
        , runs.courserun_title
    from grades
    inner join users on grades.user_id = users.user_id
    left join runs on grades.courserun_readable_id = runs.courserun_readable_id
)

select * from edxorg_grades
