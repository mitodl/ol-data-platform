-- Course Grade information for MITx Online

with grades as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserungrade') }}
)

, runs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

select
    grades.courserungrade_id
    , grades.courserun_id
    , runs.course_id
    , runs.courserun_title
    , runs.courserun_readable_id
    , runs.courserun_url
    , grades.courserungrade_grade
    , grades.courserungrade_letter_grade
    , grades.courserungrade_is_passing
    , grades.user_id
    , users.user_username
    , users.user_email
from grades
inner join runs on grades.courserun_id = runs.courserun_id
inner join users on grades.user_id = users.user_id
