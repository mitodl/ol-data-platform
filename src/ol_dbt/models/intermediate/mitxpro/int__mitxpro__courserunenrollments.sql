-- Enrollment information for MITxPro
with
    enrollments as (select * from {{ ref("stg__mitxpro__app__postgres__courses_courserunenrollment") }}),
    openedx_enrollments as (select * from {{ ref("stg__mitxpro__openedx__mysql__courserun_enrollment") }}),
    runs as (select * from {{ ref("stg__mitxpro__app__postgres__courses_courserun") }}),
    users as (select * from {{ ref("int__mitxpro__users") }}),
    mitxpro_enrollments as (
        select
            enrollments.courserunenrollment_id,
            enrollments.courserunenrollment_is_active,
            enrollments.user_id,
            enrollments.courserun_id,
            enrollments.courserunenrollment_created_on,
            enrollments.courserunenrollment_enrollment_status,
            enrollments.courserunenrollment_is_edx_enrolled,
            runs.courserun_readable_id,
            runs.courserun_title,
            runs.courserun_start_on,
            users.user_username,
            users.user_email,
            users.user_full_name,
            users.user_address_country,
            enrollments.ecommerce_company_id,
            enrollments.ecommerce_order_id,
            case
                when enrollments.ecommerce_order_id is not null
                then 'no-id-professional'
                else openedx_enrollments.courserunenrollment_enrollment_mode
            end as courserunenrollment_enrollment_mode
        from enrollments
        left join runs on enrollments.courserun_id = runs.courserun_id
        left join users on enrollments.user_id = users.user_id
        left join
            openedx_enrollments
            on runs.courserun_readable_id = openedx_enrollments.courserun_readable_id
            and users.openedx_user_id = openedx_enrollments.openedx_user_id
    )

select *
from mitxpro_enrollments
