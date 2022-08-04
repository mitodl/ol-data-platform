-- Enrollment information for MITx Online

with enrollments as (
    select * from {{ ref('stg_mitxonline__app__postgres__course_courserunenrollment') }}
)

, runs as (
    select
        id
        , title
        , courseware_url_path
    from {{ ref('stg_mitxonline__app__postgres__courses_courserun') }}
)

, users as (
    select
        id
        , username
        , email
        , is_active
    from {{ ref('stg_mitxonline__app__postgres__users_user') }}
)

, mitxonline_enrollments as (
    select
        enrollments.id
        , enrollments.active as course_run_active
        , enrollments.user_id
        , enrollments.created_on
        , enrollments.updated_on
        , runs.courseware_url_path
        , runs.title as course_title
        , users.username
        , users.email
        , users.is_active as user_active
    from enrollments
    inner join runs on enrollments.run_id = runs.id
    inner join users on enrollments.user_id = users.id
)

select * from mitxonline_enrollments
