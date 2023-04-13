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
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , courserun_title
        , courserun_readable_id
        , user_id
        , user_email
        , user_edxorg_username
        , user_username as user_mitxonline_username
    from mitxonline_enrollments

    union all

    select
        '{{ var("edxorg") }}' as platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , null as courserunenrollment_enrollment_status
        , courserun_title
        , courserun_readable_id
        , user_id
        , user_email
        , user_username as user_edxorg_username
        , user_mitxonline_username
    from edxorg_enrollments
)

select * from mitx_enrollments
