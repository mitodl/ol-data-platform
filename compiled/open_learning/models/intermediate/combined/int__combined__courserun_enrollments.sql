--- This model combines intermediate enrollments from different platform,
-- it's built as view with no additional data is stored


with mitxonline_enrollments as (
    select * from dev.main_intermediate.int__mitxonline__courserunenrollments
)

, mitxpro_enrollments as (
    select * from dev.main_intermediate.int__mitxpro__courserunenrollments
)

, bootcamps_enrollments as (
    select * from dev.main_intermediate.int__bootcamps__courserunenrollments
)

, edxorg_enrollments as (
    select * from dev.main_intermediate.int__edxorg__mitx_courserun_enrollments
)

, combined_enrollments as (
    select
        'MITx Online' as platform
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
        'xPro' as platform
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
        'Bootcamps' as platform
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
        'edX.org' as platform
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
