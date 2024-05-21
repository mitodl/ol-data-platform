with mitx_enrollments as (
    select * from {{ ref('int__mitx__courserun_enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__courserunenrollments') }}
)

, bootcamps_enrollments as (
    select * from {{ ref('int__bootcamps__courserunenrollments') }}
)

, mitx_grades as (
    select * from {{ ref('int__mitx__courserun_grades') }}
)

, mitxpro_grades as (
    select * from {{ ref('int__mitxpro__courserun_grades') }}
)

, combined_certificates as (
    select * from {{ ref('int__combined__courserun_certificates') }}
)

, combined_courseruns as (
    select * from {{ ref('int__combined__course_runs') }}
)


, combined_enrollments as (
    select
        mitx_enrollments.platform
        , mitx_enrollments.courserunenrollment_id
        , mitx_enrollments.courserunenrollment_is_active
        , mitx_enrollments.courserunenrollment_created_on
        , mitx_enrollments.courserunenrollment_enrollment_mode
        , mitx_enrollments.courserunenrollment_enrollment_status
        , mitx_enrollments.courserunenrollment_is_edx_enrolled
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_grades.courserungrade_grade
        , mitx_grades.courserungrade_is_passing
    from mitx_enrollments
    left join mitx_grades
        on
            mitx_enrollments.courserun_readable_id = mitx_grades.courserun_readable_id
            and mitx_enrollments.user_mitxonline_username = mitx_grades.user_mitxonline_username
    where mitx_enrollments.platform = '{{ var("mitxonline") }}'

    union all

    select
        mitx_enrollments.platform
        , mitx_enrollments.courserunenrollment_id
        , mitx_enrollments.courserunenrollment_is_active
        , mitx_enrollments.courserunenrollment_created_on
        , mitx_enrollments.courserunenrollment_enrollment_mode
        , mitx_enrollments.courserunenrollment_enrollment_status
        , mitx_enrollments.courserunenrollment_is_edx_enrolled
        , mitx_enrollments.user_id
        , mitx_enrollments.courserun_id
        , mitx_enrollments.courserun_title
        , mitx_enrollments.courserun_readable_id
        , mitx_enrollments.user_username
        , mitx_enrollments.user_email
        , mitx_enrollments.user_full_name
        , mitx_grades.courserungrade_grade
        , mitx_grades.courserungrade_is_passing
    from mitx_enrollments
    left join mitx_grades
        on
            mitx_enrollments.courserun_readable_id = mitx_grades.courserun_readable_id
            and mitx_enrollments.user_edxorg_username = mitx_grades.user_edxorg_username
    where mitx_enrollments.platform = '{{ var("edxorg") }}'

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , mitxpro_enrollments.courserunenrollment_id
        , mitxpro_enrollments.courserunenrollment_is_active
        , mitxpro_enrollments.courserunenrollment_created_on
        , mitxpro_enrollments.courserunenrollment_enrollment_mode
        , mitxpro_enrollments.courserunenrollment_enrollment_status
        , mitxpro_enrollments.courserunenrollment_is_edx_enrolled
        , mitxpro_enrollments.user_id
        , mitxpro_enrollments.courserun_id
        , mitxpro_enrollments.courserun_title
        , mitxpro_enrollments.courserun_readable_id
        , mitxpro_enrollments.user_username
        , mitxpro_enrollments.user_email
        , mitxpro_enrollments.user_full_name
        , mitxpro_grades.courserungrade_grade
        , mitxpro_grades.courserungrade_is_passing
    from mitxpro_enrollments
    left join mitxpro_grades
        on
            mitxpro_enrollments.courserun_readable_id = mitxpro_grades.courserun_readable_id
            and mitxpro_enrollments.user_username = mitxpro_grades.user_username

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , courserunenrollment_id
        , courserunenrollment_is_active
        , courserunenrollment_created_on
        , null as courserunenrollment_enrollment_mode
        , courserunenrollment_enrollment_status
        , null as courserunenrollment_is_edx_enrolled
        , user_id
        , courserun_id
        , courserun_title
        , courserun_readable_id
        , user_username
        , user_email
        , user_full_name
        , null as courserungrade_grade
        , null as courserungrade_is_passing
    from bootcamps_enrollments
)

select
    combined_enrollments.*
    , combined_courseruns.course_title
    , combined_courseruns.course_readable_id
    , combined_certificates.courseruncertificate_created_on
    , combined_certificates.courseruncertificate_url
    , combined_certificates.courseruncertificate_uuid
    , if(combined_certificates.courseruncertificate_url is not null, true, false) as courseruncertificate_is_earned
from combined_enrollments
left join combined_courseruns
    on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
left join combined_certificates
    on
        combined_enrollments.platform = combined_certificates.platform
        and combined_enrollments.user_username = combined_certificates.user_username
        and combined_enrollments.courserun_readable_id = combined_certificates.courserun_readable_id
