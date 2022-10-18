-- Enrollment information for MITx Online

with enrollments as (
    select * from {{ ref('stg__bootcamps__app__postgres__klasses_bootcamprunenrollment') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__klasses_bootcamprun') }}
)

, users as (
    select * from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

, bootcamps_enrollments as (
    select
        enrollments.id
        , enrollments.active as course_run_active
        , enrollments.user_id
        , enrollments.created_on
        , '' as courseware_url_path
        , runs.title as course_title
        , users.user_username
        , users.user_email
    from enrollments
    inner join runs on runs.id = enrollments.bootcamp_run_id
    inner join users on users.user_id = enrollments.user_id
)

select * from bootcamps_enrollments
