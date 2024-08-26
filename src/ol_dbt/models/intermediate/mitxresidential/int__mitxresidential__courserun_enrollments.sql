with enrollments as (
    select * from {{ ref('stg__mitxresidential__openedx__courserun_enrollment') }}
)

, runs as (
    select * from {{ ref('stg__mitxresidential__openedx__courserun') }}
)

, users as (
    select * from {{ ref('int__mitxresidential__users') }}
)

, courserun_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.user_id
        , enrollments.courserun_readable_id
        , enrollments.courserunenrollment_enrollment_mode
        , enrollments.courserunenrollment_is_active
        , enrollments.courserunenrollment_created_on
        , runs.courserun_title
        , users.user_username
        , users.user_email
        , users.user_full_name
    from enrollments
    left join runs on enrollments.courserun_readable_id = runs.courserun_readable_id
    left join users on enrollments.user_id = users.user_id
)

select * from courserun_enrollments
