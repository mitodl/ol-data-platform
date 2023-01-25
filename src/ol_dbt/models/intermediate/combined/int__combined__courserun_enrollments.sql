with mitxonline_enrollments as (
    select * from {{ ref('int__mitxonline__courserunenrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__courserunenrollments') }}
)

, bootcamps_enrollments as (
    select * from {{ ref('int__bootcamps__courserunenrollments') }}
)

, edxorg_enrollments as (
    select * from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

, combined_enrollments as (
    select
        '{{ var("mitxonline") }}' as platform
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
    from mitxonline_enrollments

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

    union all

    select
        '{{ var("edxorg") }}' as platform
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , courserunenrollment_enrollment_mode
        , null as courserunenrollment_enrollment_status
        , user_id
        , null as courserun_id
        , courserun_title
        , courserun_readable_id
        , user_username
        , user_email
    from edxorg_enrollments
)

select * from combined_enrollments
