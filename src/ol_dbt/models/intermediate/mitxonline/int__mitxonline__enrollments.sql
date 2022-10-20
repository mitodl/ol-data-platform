-- Enrollment information for MITx Online

with enrollments as (
    select * from {{ ref('stg__mitxonline__app__postgres__course_courserunenrollment') }}
)

, runs as (
    select
        courserun_id
        , courserun_title
        , courserun_readable_id
        , courserun_url
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, users as (
    select
        user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_enrollments as (
    select
        enrollments.id
        , enrollments.active as course_run_active
        , enrollments.user_id
        , enrollments.created_on
        , runs.courserun_url
        , runs.courserun_title
        , users.user_username
        , users.user_email
    from enrollments
    inner join runs on enrollments.run_id = runs.courserun_id
    inner join users on enrollments.user_id = users.user_id
)

select * from mitxonline_enrollments
