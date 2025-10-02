with combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline__course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxonline_enrollment as (
    select
        combined_enrollments.courserun_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.course_readable_id
    from combined_enrollments
    where combined_enrollments.platform = '{{ var("mitxonline") }}'
    group by
        combined_enrollments.courserun_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.course_readable_id
)

, edxorg_enrollment as (
    select
        combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.user_id
        , combined_enrollments.courserun_readable_id
        , combined_enrollments.course_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
    from combined_enrollments
    where combined_enrollments.platform = '{{ var("edxorg") }}'
)

select
    edxorg_enrollment.user_id as user_edxorg_id
    , mitx__users.user_mitxonline_id
    , edxorg_enrollment.user_email
    , mitxonline__course_runs.courserun_id
    , {{ format_course_id('edxorg_enrollment.courserun_readable_id', false) }} as courserun_readable_id
    , edxorg_enrollment.courserunenrollment_enrollment_mode
    , edxorg_enrollment.courserungrade_grade
    , edxorg_enrollment.courserungrade_is_passing
    , edxorg_enrollment.courserunenrollment_created_on
    , edxorg_enrollment.courseruncertificate_created_on
from edxorg_enrollment
left join mitxonline_enrollment
    on
        edxorg_enrollment.user_email = mitxonline_enrollment.user_email
        and edxorg_enrollment.course_readable_id = mitxonline_enrollment.course_readable_id
        and substring(edxorg_enrollment.courserun_readable_id, length(edxorg_enrollment.courserun_readable_id) - 5)
            = substring(mitxonline_enrollment.courserun_readable_id, length(mitxonline_enrollment.courserun_readable_id) - 5)
left join mitx__users
    on edxorg_enrollment.user_id = cast(mitx__users.user_edxorg_id as varchar)
left join mitxonline__course_runs
    on edxorg_enrollment.courserun_readable_id = mitxonline__course_runs.courserun_edx_readable_id
where
    edxorg_enrollment.courseruncertificate_created_on is not null
    and mitxonline_enrollment.user_email is null
