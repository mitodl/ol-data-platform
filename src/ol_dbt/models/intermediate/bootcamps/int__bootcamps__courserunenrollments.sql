-- Enrollment information for Bootcamps

with enrollments as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__courserunenrollment') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

, bootcamps_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_is_active
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_status
        , runs.courserun_title
        , users.user_username
        , users.user_email
    from enrollments
    inner join runs on runs.courserun_id = enrollments.courserun_id
    inner join users on users.user_id = enrollments.user_id
)

select * from bootcamps_enrollments
