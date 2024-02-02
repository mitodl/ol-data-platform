--- MITx course enrollments from MITx Online and edX.org with no duplication
--- For DEDP courses, verified enrollments are already handled in MITx Online
--  and edX.org enrollment intermediate models

{{ config(materialized='view') }}

with mitxonline_enrollments as (
    select *
    from {{ ref('int__mitxonline__courserunenrollments') }}
    --- to dedup, filter out migrated DEDP course enrollments for courses that run on edX.org
    where courserunenrollment_platform = '{{ var("mitxonline") }}'
)

, edxorg_enrollments as (
    select * from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

, mitx_enrollments as (
    select
        '{{ var("mitxonline") }}' as platform
        , courserunenrollment_id
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , courserunenrollment_is_edx_enrolled
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , course_number
        , courserun_start_on
        , user_id
        , user_email
        , user_full_name
        , user_username
        , user_edxorg_username
        , user_username as user_mitxonline_username
        , user_address_country
    from mitxonline_enrollments

    union all

    select
        '{{ var("edxorg") }}' as platform
        , null as courserunenrollment_id
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , null as courserunenrollment_enrollment_status
        , true as courserunenrollment_is_edx_enrolled
        , null as courserun_id
        , courserun_title
        , courserun_readable_id
        , course_number
        , courserun_start_on
        , user_id
        , user_email
        , user_full_name
        , user_username
        , user_username as user_edxorg_username
        , user_mitxonline_username
        , user_address_country
    from edxorg_enrollments
)

select * from mitx_enrollments
