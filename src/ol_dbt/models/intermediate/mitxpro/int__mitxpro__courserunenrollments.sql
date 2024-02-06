-- Enrollment information for MITxPro

with enrollments as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserunenrollment') }}
)

, runs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, mitxpro_enrollments as (
    select
        enrollments.courserunenrollment_id
        , enrollments.courserunenrollment_is_active
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserunenrollment_created_on
        , enrollments.courserunenrollment_enrollment_status
        , enrollments.courserunenrollment_is_edx_enrolled
        , runs.courserun_readable_id
        , runs.courserun_title
        , runs.courserun_start_on
        , users.user_username
        , users.user_email
        , users.user_full_name
        , users.user_address_country
        , enrollments.ecommerce_company_id
        , enrollments.ecommerce_order_id
    from enrollments
    left join runs on enrollments.courserun_id = runs.courserun_id
    left join users on enrollments.user_id = users.user_id
)

select * from mitxpro_enrollments
