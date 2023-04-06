-- Enrollment information for MITx Online

with enrollments as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__courses_courserunenrollment
)

, runs as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__courses_courserun
)

, users as (
    select
        user_id
        , user_username
        , user_email
    from dev.main_staging.stg__mitxonline__app__postgres__users_user
)

, mitxonline_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_is_active
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_enrollment_status
        , runs.courserun_title
        , runs.courserun_readable_id
        , users.user_username
        , users.user_email
    from enrollments
    inner join runs on enrollments.courserun_id = runs.courserun_id
    inner join users on enrollments.user_id = users.user_id
)

select * from mitxonline_enrollments
