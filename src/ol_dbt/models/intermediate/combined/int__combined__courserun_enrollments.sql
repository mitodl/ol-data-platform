--- This model combines intermediate enrollments from different platform,
-- it's built as view with no additional data is stored
{{ config(materialized='view') }}

with mitx_enrollments as (
    select * from {{ ref('int__mitx__courserun_enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__courserunenrollments') }}
)

, bootcamps_enrollments as (
    select * from {{ ref('int__bootcamps__courserunenrollments') }}
)

, combined_enrollments as (
    select
        platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , user_id
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , user_username
        , user_email
    from mitx_enrollments

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , user_id
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , user_username
        , user_email
    from mitxpro_enrollments

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , user_id
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , user_username
        , user_email
    from bootcamps_enrollments
)

select * from combined_enrollments
