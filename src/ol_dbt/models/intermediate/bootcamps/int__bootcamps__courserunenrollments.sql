-- Enrollment information for Bootcamps

with enrollments as (
    select * from {{ ref('stg__bootcamps__app__postgres__courserunenrollment') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, bootcamps_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_is_active
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_status
        , runs.courserun_readable_id
        , runs.courserun_title
        , runs.courserun_start_on
        , users.user_username
        , users.user_email
        , users.user_full_name
        , users.user_address_country
    from enrollments
    inner join runs on enrollments.courserun_id = runs.courserun_id
    inner join users on enrollments.user_id = users.user_id
)

select * from bootcamps_enrollments
