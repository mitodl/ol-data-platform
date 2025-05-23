with program_enrollments as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, course_enrollments as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_courses as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, aggregated_course_enrollments as (
    select
        program_courses.platform
        , program_courses.program_readable_id
        , program_courses.program_name
        , count(distinct course_enrollments.courserun_readable_id || course_enrollments.user_email) as total_enrollments
        , count(distinct course_enrollments.user_email) as unique_users
        , count(distinct course_enrollments.user_country_code) as unique_countries
        , count(
            distinct case
                when course_enrollments.courserunenrollment_enrollment_mode = 'verified'
                    then course_enrollments.courserun_readable_id || course_enrollments.user_email
            end
        ) as verified_enrollments
        , count(
            distinct case
                when course_enrollments.courserunenrollment_enrollment_mode = 'verified'
                    then course_enrollments.user_email
            end
        ) as unique_verified_users
        , count(
            distinct case
                when course_enrollments.courseruncertificate_is_earned = true
                    then course_enrollments.courserun_readable_id || course_enrollments.user_email
            end
        ) as course_certificates
        , count(
            distinct case
                when course_enrollments.courseruncertificate_is_earned = true
                    then course_enrollments.user_email
            end
        ) as unique_course_certificate_earners
    from course_enrollments
    inner join program_courses on course_enrollments.course_readable_id = program_courses.course_readable_id
    group by
        program_courses.platform
        , program_courses.program_readable_id
        , program_courses.program_name
)

, aggregated_program_certificates as (
    select
        program_type
        , program_track
        , program_readable_id
        , count(distinct case when user_has_completed_program = true then user_email end)
        as program_certificates
    from program_enrollments
    group by
        program_type
        , program_track
        , program_readable_id
)

select
    aggregated_course_enrollments.platform
    , aggregated_course_enrollments.program_name
    , aggregated_program_certificates.program_type
    , aggregated_program_certificates.program_track
    , aggregated_course_enrollments.program_readable_id
    , aggregated_course_enrollments.total_enrollments
    , aggregated_course_enrollments.unique_users
    , aggregated_course_enrollments.unique_countries
    , aggregated_course_enrollments.verified_enrollments
    , aggregated_course_enrollments.unique_verified_users
    , aggregated_course_enrollments.course_certificates
    , aggregated_course_enrollments.unique_course_certificate_earners
    , aggregated_program_certificates.program_certificates
from aggregated_course_enrollments
left join aggregated_program_certificates
    on aggregated_course_enrollments.program_readable_id = aggregated_program_certificates.program_readable_id
