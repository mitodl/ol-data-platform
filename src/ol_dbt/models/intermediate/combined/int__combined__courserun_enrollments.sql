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

, combined_certificates as (
    select * from {{ ref('int__combined__courserun_certificates') }}
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
        , user_full_name
    from mitx_enrollments

    union all

    select
        '{{ var("mitxpro") }}' as platform
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
        , user_full_name
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
        , user_full_name
    from bootcamps_enrollments
)

select
    combined_enrollments.*
    , if(combined_certificates.platform is not null, true, false) as user_has_certificate
from combined_enrollments
left join combined_certificates
    on
        combined_enrollments.platform = combined_certificates.platform
        and combined_enrollments.user_username = combined_certificates.user_username
        and combined_enrollments.courserun_readable_id = combined_certificates.courserun_readable_id
