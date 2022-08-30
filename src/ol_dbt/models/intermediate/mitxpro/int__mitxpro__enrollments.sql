-- Enrollment information for xPro

with enrollments as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserunenrollment') }}
)

, runs as (
    select
        id
        , title
        , courseware_url_path
    from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, users as (
    select
        id
        , username
        , email
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, mitxpro_enrollments as (
    select
        enrollments.id
        , enrollments.active as course_run_active
        , enrollments.user_id
        , enrollments.created_on
        , runs.courseware_url_path
        , runs.title as course_title
        , users.username
        , users.email
    from enrollments
    inner join runs on enrollments.run_id = runs.id
    inner join users on enrollments.user_id = users.id
)

select * from mitxpro_enrollments
